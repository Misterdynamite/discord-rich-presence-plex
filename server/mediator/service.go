package mediator

import (
	"context"
	"drpp/server/cache"
	"drpp/server/config"
	"drpp/server/discord"
	"drpp/server/images"
	"drpp/server/logger"
	"drpp/server/plex"
	"encoding/json/v2"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"
)

const maxUploadAttempts = 5

type ImageService interface {
	Upload(ctx context.Context, pngBytes []byte) (string, error)
	Lifespan() time.Duration
}

type Service struct {
	discordService *discord.Service
	plexServices   []*plex.Service
	cacheService   *cache.Service
	imageService   ImageService
	imagesConfig   config.Images
	discordConfig  config.Discord

	stateMu          sync.Mutex
	state            string
	stateChangedAtMs int64
	stopTimer        *time.Timer

	activityCh chan *plex.Activity
	mu         sync.Mutex
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewService(
	discordService *discord.Service,
	plexServices []*plex.Service,
	cacheService *cache.Service,
	imageService ImageService,
	imagesConfig config.Images,
	discordConfig config.Discord,
) *Service {
	return &Service{
		discordService: discordService,
		plexServices:   plexServices,
		cacheService:   cacheService,
		imageService:   imageService,
		imagesConfig:   imagesConfig,
		discordConfig:  discordConfig,
	}
}

func (s *Service) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return
	}
	s.activityCh = make(chan *plex.Activity, 1)
	for _, plexService := range s.plexServices {
		plexService.Start(s.plexCallback)
	}
	var ctx context.Context
	ctx, s.cancel = context.WithCancel(context.Background())
	s.wg.Go(func() {
		var handlerWg sync.WaitGroup
		var handlerCtx context.Context
		var cancelHandler context.CancelFunc
		for {
			select {
			case <-ctx.Done():
				handlerWg.Wait()
				return
			case activity, open := <-s.activityCh:
				if !open {
					return
				}
				if cancelHandler != nil {
					cancelHandler()
					handlerWg.Wait()
					if ctx.Err() != nil {
						return
					}
				}
				select {
				case newActivity, open := <-s.activityCh:
					if !open {
						return
					}
					activity = newActivity
				default:
				}
				handlerWg.Add(1)
				handlerCtx, cancelHandler = context.WithCancel(ctx) //nolint:fatcontext
				go func(handlerCtx context.Context, cancelHandler context.CancelFunc) {
					s.handlePlexActivity(handlerCtx, activity)
					cancelHandler()
					handlerWg.Done()
				}(handlerCtx, cancelHandler)
			}
		}
	})
}

func (s *Service) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel == nil {
		return
	}
	s.cancel()
	s.cancel = nil
	for _, plexService := range s.plexServices {
		plexService.Stop()
	}
	close(s.activityCh)
	s.wg.Wait()
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.stopActivity()
}

func (s *Service) stopActivity() {
	s.state = ""
	s.clearStopTimer()
	s.discordService.Disconnect()
}

func (s *Service) setStopTimer(duration time.Duration) {
	if s.stopTimer != nil {
		return
	}
	s.stopTimer = time.AfterFunc(duration, func() {
		s.stateMu.Lock()
		defer s.stateMu.Unlock()
		s.stopActivity()
	})
}

func (s *Service) clearStopTimer() {
	if s.stopTimer == nil {
		return
	}
	s.stopTimer.Stop()
	s.stopTimer = nil
}

func (s *Service) plexCallback(activity *plex.Activity) {
	// Drain any stale pending activity
	select {
	case <-s.activityCh:
	default:
	}
	// Send the new activity, or drop if another activity filled the channel at the same time
	select {
	case s.activityCh <- activity:
	default:
	}
}

func (s *Service) handlePlexActivity(ctx context.Context, activity *plex.Activity) {
	activityJson, _ := json.Marshal(activity)
	logger.Info("Activity: %s", activityJson)
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.state != activity.State {
		s.stateChangedAtMs = time.Now().UnixMilli()
	}
	if activity.State == "stopped" {
		if s.state == "" || s.state == "stopped" {
			// Stopped without a previous state or timer already set, ignore
			return
		}
		s.state = "stopped"
		s.clearStopTimer()
		s.setStopTimer(time.Duration(s.discordConfig.StopTimeoutSeconds) * time.Second)
		return
	}
	var rule config.DisplayRule
	switch activity.MediaType {
	case "movie":
		rule = s.discordConfig.DisplayRules.Movie
	case "episode":
		rule = s.discordConfig.DisplayRules.Episode
	case "track":
		rule = s.discordConfig.DisplayRules.Track
	case "clip":
		rule = s.discordConfig.DisplayRules.Clip
	case "liveEpisode":
		rule = s.discordConfig.DisplayRules.LiveEpisode
	default:
		logger.Error(nil, "Invalid media type %q", activity.MediaType)
		return
	}
	if activity.State == "paused" && rule.PauseTimeoutSeconds >= 0 {
		if s.state == "" {
			// Paused without a previous state, with pause timeout set, ignore
			return
		}
		if rule.PauseTimeoutSeconds == 0 {
			// Paused, with pause timeout set to 0, stop immediately
			s.stopActivity()
			return
		}
	}
	// Playing, or transitioned to paused, or pause timeout set to -1, so clear idle timer
	if activity.State == "playing" || s.state != "paused" || rule.PauseTimeoutSeconds < 0 {
		s.clearStopTimer()
	}
	templateData := buildTemplateData(activity)
	logger.Debug("Template: %#v", templateData)
	var activityType discord.ActivityType
	if activity.MediaType == "track" {
		activityType = discord.ActivityTypeListening
	} else {
		activityType = discord.ActivityTypeWatching
	}
	var activityStatusDisplayType discord.ActivityStatusDisplayType
	statusType := renderTemplate(rule.StatusType, templateData)
	switch statusType {
	case "details":
		activityStatusDisplayType = discord.ActivityStatusDisplayTypeDetails
	case "state":
		activityStatusDisplayType = discord.ActivityStatusDisplayTypeState
	case "name":
		activityStatusDisplayType = discord.ActivityStatusDisplayTypeName
	default:
		logger.Error(nil, "Invalid status type %q, defaulting to %q", statusType, "name")
		activityStatusDisplayType = discord.ActivityStatusDisplayTypeName
	}
	resolveImage := func(tmpl string) string {
		thumb := renderTemplate(tmpl, templateData)
		if thumb == "" {
			return ""
		}
		if thumb == activity.Item.Thumb || thumb == activity.ParentItem.Thumb || thumb == activity.GrandparentItem.Thumb {
			var sourceUrl string
			var headers map[string]string
			if parsed, err := url.Parse(thumb); err != nil || !parsed.IsAbs() || (parsed.Scheme != "http" && parsed.Scheme != "https") {
				sourceUrl, headers = activity.GetThumbUrl(thumb)
			} else {
				sourceUrl = thumb
			}
			return s.getUploadedImageUrl(ctx, thumb, sourceUrl, headers)
		}
		return thumb
	}
	var largeImage, smallImage string
	var imageWg sync.WaitGroup
	imageWg.Go(func() { largeImage = resolveImage(rule.LargeImage) })
	imageWg.Go(func() { smallImage = resolveImage(rule.SmallImage) })
	imageWg.Wait()
	discordActivity := &discord.Activity{
		Name:              activityText(renderTemplate(rule.Name, templateData), 128),
		Type:              activityType,
		StatusDisplayType: activityStatusDisplayType,
		Details:           activityText(renderTemplate(rule.Details, templateData), 128),
		DetailsUrl:        activityUrl(renderTemplate(rule.DetailsUrl, templateData), 256),
		State:             activityText(renderTemplate(rule.State, templateData), 128),
		StateUrl:          activityUrl(renderTemplate(rule.StateUrl, templateData), 256),
		Assets: discord.ActivityAssets{
			LargeImage: activityUrl(largeImage, 300),
			LargeText:  activityText(renderTemplate(rule.LargeText, templateData), 128),
			LargeUrl:   activityUrl(renderTemplate(rule.LargeUrl, templateData), 256),
			SmallImage: activityUrl(smallImage, 300),
			SmallText:  activityText(renderTemplate(rule.SmallText, templateData), 128),
			SmallUrl:   activityUrl(renderTemplate(rule.SmallUrl, templateData), 256),
		},
	}
	progressMode := renderTemplate(rule.ProgressMode, templateData)
	switch progressMode {
	case "off", "elapsed", "remaining", "bar", "state":
	default:
		logger.Error(nil, "Invalid progress mode %q, defaulting to %q", progressMode, "off")
		progressMode = "off"
	}
	if progressMode != "off" {
		if progressMode == "state" {
			discordActivity.Timestamps.StartMs = s.stateChangedAtMs
		} else {
			now := time.Now().UnixMilli()
			if progressMode == "bar" || progressMode == "elapsed" {
				discordActivity.Timestamps.StartMs = now - activity.ElapsedDurationMs
			}
			if progressMode == "bar" || progressMode == "remaining" {
				discordActivity.Timestamps.EndMs = now + activity.Item.DurationMs - activity.ElapsedDurationMs
			}
		}
	}
	for _, button := range rule.Buttons {
		label := activityText(renderTemplate(button.Label, templateData), 32)
		url := activityUrl(renderTemplate(button.Url, templateData), 512)
		if url == "" {
			continue
		}
		discordActivity.Buttons = append(discordActivity.Buttons, discord.ActivityButton{Label: label, Url: url})
		if len(discordActivity.Buttons) == 2 {
			break
		}
	}
	if err := s.discordService.SetActivity(discordActivity); err != nil {
		logger.Error(err, "Failed to set Discord activity")
	}
	if activity.State == "paused" && rule.PauseTimeoutSeconds > 0 {
		s.setStopTimer(time.Duration(rule.PauseTimeoutSeconds) * time.Second)
	} else {
		s.setStopTimer(time.Duration(s.discordConfig.IdleTimeoutSeconds) * time.Second)
	}
	s.state = activity.State
}

func activityText(text string, maxLength int) string {
	return activityField(text, maxLength, "...", 2, " ")
}

func activityUrl(text string, maxLength int) string {
	return activityField(text, maxLength, "", 0, "")
}

func activityField(text string, maxLength int, ellipsis string, minLength int, padding string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return text
	}
	text = adjustLength(text, maxLength, ellipsis, minLength, padding)
	// Discord sometimes counts bytes, not runes, so truncate to maxLength bytes without breaking UTF-8 encoding
	if maxLength <= 0 || len(text) <= maxLength {
		return text
	}
	return strings.ToValidUTF8(text[:maxLength], "")
}

// TODO: Maybe use https://pkg.go.dev/golang.org/x/sync/singleflight instead of the custom implementation below

type pendingUpload struct {
	done chan struct{}
	url  string
}

var pendingUploads = map[string]*pendingUpload{}
var pendingUploadsMu sync.Mutex

func (s *Service) getUploadedImageUrl(ctx context.Context, thumb string, sourceUrl string, headers map[string]string) string { //nolint:contextcheck // Image upload has to continue in the background so don't inherit the context
	if s.imageService == nil {
		return ""
	}
	cacheKey := fmt.Sprintf("%s:%t:%d", thumb, s.imagesConfig.FitInSquare, s.imagesConfig.MaxSize)
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.imagesConfig.UploadTimeoutSeconds)*time.Second)
	defer cancel()
	for attempt := 1; attempt <= maxUploadAttempts; attempt++ {
		if cached := s.cacheService.Get(cacheKey); cached != "" {
			return cached
		}
		pendingUploadsMu.Lock()
		result, ok := pendingUploads[cacheKey]
		if !ok {
			logger.Debug("Initiating upload for image %q", thumb)
			result = &pendingUpload{done: make(chan struct{}), url: ""}
			pendingUploads[cacheKey] = result
			imgCtx, cancel := context.WithTimeout(context.Background(), time.Duration(s.imagesConfig.UploadTimeoutSeconds)*time.Second)
			go func() {
				defer cancel()
				defer func() {
					pendingUploadsMu.Lock()
					delete(pendingUploads, cacheKey)
					pendingUploadsMu.Unlock()
					close(result.done)
				}()
				pngBytes, err := images.GetPngBytes(imgCtx, sourceUrl, s.imagesConfig.FitInSquare, s.imagesConfig.MaxSize, headers)
				if err != nil {
					logger.Error(err, "Failed to get image %q for uploading", thumb)
					return
				}
				newUrl, err := s.imageService.Upload(imgCtx, pngBytes)
				if err != nil || newUrl == "" {
					logger.Error(err, "Failed to upload image %q", thumb)
					return
				}
				if err := s.cacheService.Set(cacheKey, newUrl, s.imageService.Lifespan()); err != nil {
					logger.Error(err, "Failed to add uploaded image %q URL to cache", thumb)
				}
				result.url = newUrl
			}()
		}
		pendingUploadsMu.Unlock()
		select {
		case <-ctx.Done():
			return ""
		case <-result.done:
			if result.url != "" {
				return result.url
			}
			select {
			case <-ctx.Done():
				return ""
			case <-time.After(1 * time.Second):
			}
		}
	}
	return ""
}

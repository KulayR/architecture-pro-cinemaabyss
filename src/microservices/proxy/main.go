package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

func loadConfig() *Config {
	return &Config{
		Port:                   getEnv("PORT", "8000"),
		MonolithURL:            getEnv("MONOLITH_URL", "http://monolith:8080"),
		MoviesServiceURL:       getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"),
		EventsServiceURL:       getEnv("EVENTS_SERVICE_URL", "http://events-service:8082"),
		GradualMigration:       getEnv("GRADUAL_MIGRATION", "false") == "true",
		MoviesMigrationPercent: getEnvInt("MOVIES_MIGRATION_PERCENT", 0),
	}
}

type ProxyHandler struct {
	monolithProxy *httputil.ReverseProxy
	moviesProxy   *httputil.ReverseProxy
	eventsProxy   *httputil.ReverseProxy
	config        *Config
}

func NewProxyHandler(cfg *Config) *ProxyHandler {
	return &ProxyHandler{
		monolithProxy: newReverseProxy(cfg.MonolithURL),
		moviesProxy:   newReverseProxy(cfg.MoviesServiceURL),
		eventsProxy:   newReverseProxy(cfg.EventsServiceURL),
		config:        cfg,
	}
}

// ServeHTTP реализует логику Strangler Fig
func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// 1. Логика для сервиса Events (простое перенаправление, если требуется)
	if strings.HasPrefix(path, "/api/events") {
		log.Printf("[Proxy] Routing to Events Service: %s", path)
		h.eventsProxy.ServeHTTP(w, r)
		return
	}

	// 2. Логика миграции Movies (Strangler Fig)
	// Предполагаем, что эндпоинты фильмов начинаются с /movies
	if strings.HasPrefix(path, "/api/movies") {
		if h.shouldMigrate() {
			log.Printf("[Proxy] 🟢 Strangler Fig: Routing to Microservice (Movies): %s", path)
			h.moviesProxy.ServeHTTP(w, r)
			return
		}
		log.Printf("[Proxy] 🔴 Legacy: Routing to Monolith (Movies): %s", path)
		h.monolithProxy.ServeHTTP(w, r)
		return
	}

	// 3. Весь остальной трафик идет в Монолит
	log.Printf("[Proxy] Default: Routing to Monolith: %s", path)
	h.monolithProxy.ServeHTTP(w, r)
}

// shouldMigrate определяет, нужно ли отправить запрос в новый микросервис
func (h *ProxyHandler) shouldMigrate() bool {
	if !h.config.GradualMigration {
		return false
	}
	// Генерируем число от 0 до 99
	roll := rand.Intn(100)
	return roll < h.config.MoviesMigrationPercent
}

// Вспомогательные функции

func newReverseProxy(targetStr string) *httputil.ReverseProxy {
	target, err := url.Parse(targetStr)
	if err != nil {
		log.Fatalf("Error parsing URL %s: %v", targetStr, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	// Обновляем Director, чтобы правильно устанавливать заголовки Host
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host // Важно для корректной работы некоторых web-серверов
	}

	return proxy
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if valueStr, exists := os.LookupEnv(key); exists {
		if value, err := strconv.Atoi(valueStr); err == nil {
			return value
		}
	}
	return fallback
}

func main() {
	// Инициализация случайного зерна (для Go < 1.20, но не помешает)
	rand.Seed(time.Now().UnixNano())

	config := loadConfig()
	handler := NewProxyHandler(config)

	log.Printf("Starting Proxy Service on port %s", config.Port)
	log.Printf("Config: Migration=%v, Percent=%d%%", config.GradualMigration, config.MoviesMigrationPercent)
	log.Printf("Monolith: %s | Movies: %s | Events: %s", config.MonolithURL, config.MoviesServiceURL, config.EventsServiceURL)

	// Запускаем сервер, перенаправляя все запросы в наш handler
	http.Handle("/", handler)
	if err := http.ListenAndServe(":"+config.Port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
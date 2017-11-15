(ns ajure.constants)

(def document-fetching-strategies #{:if-none-match :if-match})
(def error-types #{:malformed-input :malformed-output :api-not-found :api-bad-request :api-server-error :api-forbidden :unknown})
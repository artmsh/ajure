(ns ajure.handlers
  (:require [cheshire.core :as json]
            [clojure.string :as str]))

(defn default-return-result [_ body _]
  {:success (get (json/parse-string body) "result")})

(defn get-boundary [content-type]
  (second (first (re-seq #"boundary=([0-9a-z]+)" content-type))))

(defn get-status-headers-body [req lines]
  (let [status-line (nth lines 3)
        status (read-string (second (str/split status-line #" ")))
        [header-lines body-lines]
                       (->> lines
                         (drop 4)
                         (split-with #(not= "" %)))

        body (second body-lines)
        headers (into {} (map #(str/split % #": " 2) header-lines))
        handler (get-in req [:responses status])]
    (handler status body headers)))

(defn batch-parse-result [reqs _ body headers]
  (let [boundary (str "--" (get-boundary (get headers "content-type")))
        splitted (->>
                   (str/split body (re-pattern boundary))
                   (drop-last)
                   (drop 1)
                   (map #(str/split % #"\r\n")))
        mapped (map get-status-headers-body reqs splitted)]
    {:success mapped}))

(defn body-json-success [_ body _] {:success (json/parse-string body)})
(defn body-json-error [_ body-str _]
  (let [body (json/parse-string body-str)]
    {:error {:type (keyword (str "api-error-" (get body "errorNum")))
             :message (get body "errorMessage")}}))

(defn not-found [message] {:error {:type :api-not-found :message message}})
(defn bad-request [message] {:error {:type :api-bad-request :message message}})
(defn forbidden [message] {:error {:type :api-forbidden :message message}})
(defn server-error [] {:error {:type :api-server-error :message "Internal server error"}})
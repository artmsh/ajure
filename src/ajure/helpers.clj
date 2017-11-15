(ns ajure.helpers
  (:require [clojure.spec.alpha :as s]
            [byte-streams :as bs]
            [expound.alpha :as expound]))

(defn get-code-and-body-from-request [rq]
  (let [http-status-code (:status rq)
        body-str (-> rq :body bs/to-string)]
    [http-status-code body-str]))

(defn spec-valid? [[key val]]
  (s/valid? (keyword (str (find-ns 'ajure.specs)) (str key)) val))

(defn spec-explain [[key val]]
  (expound/expound-str (keyword (str (find-ns 'ajure.specs)) (str key)) val))

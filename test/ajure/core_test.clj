(ns ajure.core-test
  (:require [clojure.test :refer :all]
            [ajure.core :refer :all]
            [ajure.protocols :refer :all]
            [ajure.pull :refer :all]
            [clj-aql.core :refer :all]
            [clojure.spec.alpha :as s]
            [expound.alpha :as expound])
  (:import (ajure ArangodbApi)
           (ajure ArangodbPullApi)))

(def api (ArangodbApi. "http://arangodb.tom.dev"))
(def pull-api (ArangodbPullApi. api))


(defn is-eq-map? [expected actual-full]
  (let [call-result (:success actual-full)]
    (if (nil? call-result)
      false
      (= expected (select-keys call-result (keys expected))))))


(deftest db-test

  (testing "Getting current database"
    (let [res (get-current-database api)]
      (is (= "_system" (get-in res [:success "name"])))))

  (testing "Full integration"
    (let [dbs-before (get-all-databases api)
          cd-testdb (create-database api "testdb")
          cc-test-coll (create-collection api "testdb" {:name "test_collection"})
          gc-testdb (get-collections api "testdb")
          cd-0 (create-document api "testdb" "test_collection" {:_key "0" :a 1 :b 2} {})
          gc-test-coll (get-collection api "testdb" "test_collection")
          ed-0 (exist-document? api "testdb" "test_collection/0")
          id-1-0 (import-documents api "testdb" [{:_key "1" :a 2 :c -1} {:_key "0" :a 3}]
                                   {:collection "test_collection" :type "list" :onDuplicate "update"})
          gcp-test-coll (get-collection-properties api "testdb" "test_collection")
          ed-1 (exist-document? api "testdb" "test_collection/1")
          gd-0 (get-document api "testdb" "test_collection/0")
          rd-0 (remove-document api "testdb" "test_collection/0" {})
          rc-test-coll (remove-collection api "testdb" "test_collection" {})
          rd-testdb (remove-database api "testdb")
          dbs-after (get-all-databases api)
          ]
      (is (= true (:success cd-testdb)))
      (is (is-eq-map?
            {"name" "test_collection" "waitForSync" false "isSystem" false "isVolatile" false "status" 3 "type" 2}
            cc-test-coll))
      (is (is-eq-map?
            {"name" "test_collection" "isSystem" false "status" 3 "type" 2}
            {:success (first (filter #(= (get % "isSystem") false) (:success gc-testdb)))}))
      (is (is-eq-map?
            {"_id" "test_collection/0" "_key" "0"}
            cd-0))
      (is (is-eq-map?
            {"name" "test_collection" "isSystem" false "status" 3 "type" 2}
            gc-test-coll))
      (is (= true (:success ed-0)))
      (is (is-eq-map?
            {"error" false "created" 1 "errors" 0 "empty" 0 "updated" 1 "ignored" 0}
            id-1-0))
      (is (is-eq-map?
            {"name" "test_collection"
             "isSystem" false
             "doCompact" true
             "isVolatile" false
             "keyOptions" { "type" "traditional" "allowUserKeys" true "lastValue" 1 }
             "waitForSync" false
             "status" 3
             "type" 2}
            gcp-test-coll))
      (is (= true (:success ed-1)))
      (is (is-eq-map?
            {"_id" "test_collection/0" "_key" "0" "a" 3 "b" 2}
            gd-0))
      (is (is-eq-map?
            {"_id" "test_collection/0" "_key" "0"}
            rd-0))
      (is (is-eq-map?
            {"id" (get-in cc-test-coll [:success "id"]) "error" false}
            rc-test-coll))
      (is (= true (:success rd-testdb)))
      (is (= dbs-before dbs-after))
      ))

  (testing "AQL Queries"
    (prn (create-cursor api "tom" (FOR [l] :IN "location" (RETURN l)) {}))
    (prn (create-cursor api "tom" (RETURN (VALUES (KEEP (DOCUMENT "location/t1p") ["id" "locationType"]))) {})))

  (testing "Pull API"
    (prn (pull pull-api '[:body/id :body/barcode :attributes/_body_id
                          {:body/descriptions [:description/id :description/value]}] ["tom" "body/00073480000341"])))
  (testing "Batch API"
    (get-document api "tom" "location/t1p")
    (get-collections api "tom")
    (prn (batch api "tom" ["GET /_api/version HTTP/1.1" "GET /_api/document/location/t1p HTTP/1.1"]))
    ;(batch api "tom" [(get-version) (get-document "location/t1p")])

    )
  )
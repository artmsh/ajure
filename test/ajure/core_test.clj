(ns ajure.core-test
  (:require [clojure.test :refer :all]
            [ajure.core :refer :all]
            [ajure.protocols :refer :all]
            [ajure.pull :refer :all]
            [clj-aql.core :refer :all]
            [clojure.spec.alpha :as s]
            [expound.alpha :as expound]
            [ajure.requests :as reqs]
            [clojure.string :as str])
  (:import (ajure.core ArangodbApi)
           (ajure.pull ArangodbPullApi)))

(def api (ArangodbApi. "http://arangodb.tom.development"))
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
          ed-1_ (exist-document? api "testdb" "test_collection/1")
          id-1-0 (import-documents api "testdb" [{:_key "1" :a 2 :c -1} {:_key "0" :a 3}]
                                   {:collection "test_collection" :type "list" :onDuplicate "update"})
          gcp-test-coll (get-collection-properties api "testdb" "test_collection")
          ed-1 (exist-document? api "testdb" "test_collection/1")
          gd-0 (get-document api "testdb" "test_collection/0")
          uds-0 (update-documents api "testdb" "test_collection" [{:_key "0" :a 2}] {})
          rd-0 (remove-document api "testdb" "test_collection/0" {})
          rc-test-coll (remove-collection api "testdb" "test_collection" {})
          rd-testdb (remove-database api "testdb")
          dbs-after (get-all-databases api)
          ]
      (is (= {:success true} cd-testdb))
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
      (is (= {:success true} ed-0))
      (is (= {:success false} ed-1_))
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
      (is (= {:success true} ed-1))
      (is (is-eq-map?
            {"_id" "test_collection/0" "_key" "0" "a" 3 "b" 2}
            gd-0))
      (is (= {:success (list {"_id" "test_collection/0" "_key" "0"})}
             (update uds-0 :success #(map (fn [d] (select-keys d #{"_id" "_key"})) %))))
      (is (is-eq-map?
            {"_id" "test_collection/0" "_key" "0"}
            rd-0))
      (is (is-eq-map?
            {"id" (get-in cc-test-coll [:success "id"]) "error" false}
            rc-test-coll))
      (is (= true (:success rd-testdb)))
      (is (= dbs-before dbs-after))))

  (testing "AQL Queries"
    (prn (create-cursor api "tom" (FOR [l] :IN "location" (RETURN l)) {}))
    (prn (create-cursor api "tom" (RETURN (VALUES (KEEP (DOCUMENT "location/t1p") ["id" "locationType"]))) {})))

  (testing "Pull API"
    (prn (pull pull-api '[:body/id :body/barcode :attributes/_body_id
                          {:body/descriptions [:description/id :description/value]}] ["tom" "body/00073480000341"])))
  (testing "Batch API"
    (get-document api "tom" "location/t1p")
    (get-collections api "tom")
    (prn (batch api "tom" [(reqs/get-api-version)
                           (reqs/get-document "location/t1p")]))
    (is (= (batch api "tom" [(reqs/exist-document? "location/t1p")
                             (reqs/exist-document? "location/never-ever-exist")])
           {:success [{:success true} {:success false}]})))

  (testing "Simple API"
    (prn (get-by-keys api "tom" "location" ["bfresh-allston"]))))
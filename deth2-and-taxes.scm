(import http-client
        intarweb
        openssl
        uri-common
        medea
        format
        args
        (only srfi-1 alist-cons)
        srfi-1
        srfi-121
        srfi-197
        srfi-19
        srfi-13
        bindings
        scheme
        (chicken process-context)
        (chicken port)
        (chicken string)
        (chicken io))

(define (with-query-params url params)
  (parameterize ((form-urlencoded-separator "&"))
    (update-uri url query: params)))

(define cryptocompare-api-key (make-parameter (get-environment-variable "CRYPTOCOMPARE_API_KEY")))

(define (cryptocompare-auth-header)
  (format #f "ApiKey ~A" (cryptocompare-api-key)))

(define cryptocompare-base-url
  (uri-reference "https://min-api.cryptocompare.com/"))

(define histohour-path '(/ "data" "v2" "histohour"))

(define (request-price timestamp)
  (let* ((uri (chain cryptocompare-base-url
                     (update-uri _ path: histohour-path)
                     (with-query-params _ `((fsym . "ETH")
                                            (tsym .  "USD")
                                            (toTs . ,(format #f "~D" timestamp))
                                            (limit . 1)))))
         (request (make-request method: 'GET
                                uri: uri
                                headers: (headers `((Authorization ,(cryptocompare-auth-header)))))))
    (condition-case 
        (with-input-from-request request #f read-json)
      [(exn http err) (format #t "~A ~A" http err)])))

(define (vector-last vec)
  (vector-ref vec (- (vector-length vec) 1)))

(define (avg-price response)
  (let* ((price-data (chain response
                            (alist-ref 'Data _)
                            (alist-ref 'Data _)
                            (vector-last _)))
         (high (alist-ref 'high price-data))
         (low (alist-ref 'low price-data))
         (time (alist-ref 'time price-data)))
    (cons time (/ (+ high low) 2))))

(define (get-historical-price timestamp)
  (chain (request-price timestamp)
         (avg-price _)))

(define eth2-base-url
  (uri-reference (get-environment-variable "ETH2_BASE_URL")))

(define eth2-genesis-path '(/ "eth" "v1" "beacon" "genesis"))

(define (eth2-validators-path state-id validator)
  `(/ "eth" "v1" "beacon" "states" ,state-id "validators" ,validator))

(define (request-genesis-block)
  (let* ((uri (update-uri eth2-base-url path: eth2-genesis-path))
         (request (make-request method: 'GET uri: uri)))
    (condition-case 
        (with-input-from-request request #f read-json)
      [(exn http err) (format #t "~A ~A" http err)])))

#;(define genesis (request-genesis-block))
#;(define genesis-time
  (chain genesis
         (alist-ref 'data _)
         (alist-ref 'genesis_time _)
         (string->number _)))
(define genesis-time 1606824023)
(define slot-duration 12)
(define slots-per-epoch 32)

(define (slot->seconds slot)
  (let ((time-since-genesis (* slot-duration slot)))
    (+ genesis-time time-since-genesis)))

(define (epoch->slots epoch)
  (* epoch slots-per-epoch))

(define (seconds->slot seconds)
  (let ((time-since-genesis (- seconds genesis-time)))
    (/ time-since-genesis slot-duration)))

(define (date->slot date)
  (seconds->slot
   (date->seconds date)))

(define last-date (make-date 0 0 0 0 1 1 2021))
(define last-slot (make-parameter (- (date->slot last-date) 1)))
(define first-slot (make-parameter 0))

 ;; Default: 12 hours (in slots)
(define granularity (make-parameter (/ (* 60 60 12) slot-duration)))

(define (fi int)
  (format #f "~D" int))

(define (ff flonum)
  (format #f "~,,F" flonum))

;; How many hours between genesis and last-date?
#;(fi (/  (- (date->seconds last-date) genesis-time) 3600))

(define (enumerate-slots first-slot last-slot granularity)
  (generator->list
   (make-range-generator first-slot last-slot granularity)))

(define validator-index (make-parameter 17202))

(define (request-validator-info slot validator)
  (let* ((uri (chain eth2-base-url
                     (update-uri _ path: (eth2-validators-path slot validator))))
         (request (make-request method: 'GET uri: uri)))
    (condition-case 
        (with-input-from-request request #f read-json)
      [(exn http err) (format #t "~A ~A" http err)])))

(define (gwei->eth gwei)
  (/ gwei 1000000000))

(define (eth->gwei eth)
  (* eth 1000000000))

(define (fmt-gwei->eth gwei)
 (format #f "~,,-9f" gwei))

(define (get-validator-balance slot validator)
  (let ((resp (request-validator-info
               (if (number? slot) (number->string slot) slot)
               (number->string validator))))
    (chain resp
           (alist-ref 'data _)
           (alist-ref 'balance _)
           (string->number _))))

(define fetch-prices? (make-parameter #f))

(define (get-balance-change validator slot last)
  (let* ((balance (get-validator-balance slot validator))
         (timestamp (slot->seconds slot))
         (last-balance (alist-ref 'balance last))
         (balance-change (- balance last-balance)))
    `((validator . ,validator)
      (slot . ,slot)
      (timestamp . ,timestamp)
      (balance . ,balance)
      (balance-change . ,balance-change))))

(define (with-prices bc)
  (let* ((balance-change (alist-ref 'balance-change bc))
         (timestamp (alist-ref 'timestamp bc))
         (price-info (get-historical-price timestamp))
         (price-timestamp (car price-info))
         (price-usd (cdr price-info))
         (balance-change-usd (* balance-change price-usd)))
    (chain bc
           (alist-cons 'price-timestamp price-timestamp _)
           (alist-cons 'price-usd price-usd _)
           (alist-cons 'balance-change-usd balance-change-usd _))))

(define (make-initial-balance-change validator)
  `((validator . ,validator)
    (slot . #f)
    (timestamp . #f)
    (balance . ,(eth->gwei 32))
    (balance-change . 0)
    (balance-change-usd . 0)
    (price-usd . 0)
    (price-timestamp . 0)))

(define (get-balance-changes validator first-slot last-slot granularity)
  (let ((slots (enumerate-slots first-slot last-slot granularity))
        (last (make-parameter (make-initial-balance-change validator))))
    (map (lambda (slot)
           (format #t "Fetching balance changes for validator ~A at slot ~A\n" validator slot)
           (let ((balance-change (if (fetch-prices?)
                                     (with-prices
                                      (get-balance-change validater slot (last)))
                                     (get-balance-change validator slot (last)))))
             (last balance-change)
             balance-change))
         slots)))

(define (alist-replace k fn list)
  (alist-update k (fn (alist-ref k list)) list))

(define (with-formatted-amounts balance-change)
  (chain balance-change
         (alist-replace 'balance fmt-gwei->eth _)
         (alist-replace 'balance-change fmt-gwei->eth _)
         (alist-replace 'balance-change-usd fmt-gwei->eth _)))

(define (make-snapshot balance-changes path)
  (call-with-output-file path
    (lambda (out)
      (write balance-changes out))))

(define (load-snapshot path)
  (call-with-input-file path
    (lambda (in)
      (read in))))

(define (fmt-timestamp-csv seconds)
  (chain seconds
         (seconds->date _)
         (date->string _ "~Y-~m-~d ~H:~M ~Z")))

(define (balance-change->csv-row bc)
  `(,(fmt-timestamp-csv (alist-ref 'timestamp bc))
    ""
    ""
    ,(fmt-gwei->eth (alist-ref 'balance-change bc))
    "ETH"
    "staking"))

(define (write-to-csv balance-changes path)
  (let ((header '("Date" "Sent Amount" "Sent Currency" "Received Amount" "Received Currency" "Label"))
        (delimeter ","))
    (call-with-output-file path
      (lambda (out)
        (write-line (string-join header delimeter) out)
        (for-each
         (lambda (bc)
           (write-line
            (string-join (balance-change->csv-row bc) delimeter)
            out))
         balance-changes)))))

(define out-path (make-parameter nil))

(define opts
  (list (args:make-option (i validator-index) #:required "Validator index to get balances for. Required.")
        (args:make-option (s start-date) #:required "Start date for pulling balances in yyyy-mm-dd format. \nIf not present, defaults to genesis.")
        (args:make-option (e end-date) #:required "End date for pulling balances. If not present, defaults \nto current date.")
        (args:make-option (p fetch-prices) #:none "If present, fetch prices from CryptoCompare. The CRYPTOCOMPARE_API_KEY envvar must also be set.")
        (args:make-option (g granularity) #:required "In seconds. Required.")
        (args:make-option (o out-path) #:required "Path where CSV should be written. Required.")
        (args:make-option (h help) #:none "Display this text"
                          (help)
                          (exit 0))))

(define (help)
  (with-output-to-port (current-error-port)
    (lambda ()
      (print "Usage: " (car (argv)) " [options...]")
      (newline)
      (parameterize ((args:width 35))
        (print (args:usage opts))))))

(define (usage)
  (help)
  (exit 1))

(define (main)
  (format #t "Validator: ~A\n" (validator-index))
  (format #t "First slots: ~D\n" (first-slot))
  (format #t "Last slots: ~D\n" (last-slot))
  (format #t "Granularity: ~A\n" (granularity))
  (let ((balance-changes (get-balance-changes (validator-index) (first-slot) (last-slot) (granularity)))) ;
  (write-to-csv balance-changes (out-path)))
  (format #t "Done."))

(receive (options operands)
    (args:parse (command-line-arguments) opts)
  (begin
    (when (not (and (alist-ref 'out-path options)
                    (alist-ref 'validator-index options)
                    (alist-ref 'granularity options)))
      (format (current-error-port) "--out-path, --validator-index, and --granularity are required parameters.~%")
      (usage))

    (when (and (alist-ref 'fetch-prices options)
               (not (cryptocompare-api-key)))
      (format (current-error-port) "CRYPTOCOMPARE_API_KEY envvar must be set to fetch prices.~%")
      (usage))

    (when (not eth2-base-url)
      (format (current-error-port) "ETH2_BASE_URL envvar must be set.~%")
      (usage))

    (let* ((validator (string->number (alist-ref 'validator-index options)))
           (start-date-str (alist-ref 'start-date options))
           (start-slot (if start-date-str
                           (date->slot (scan-date start-date-str "~Y-~m-~d"))
                           0))
           (end-date-str (alist-ref 'end-date options))
           (end-slot (if end-date-str
                         (date->slot (scan-date end-date-str "~Y-~m-~d"))
                         (date->slot (current-date))))
           (granularity-in-slots (/ (string->number (alist-ref 'granularity options)) slot-duration)))
      (parameterize ((validator-index validator)
                     (first-slot start-slot)
                     (last-slot end-slot)
                     (fetch-prices? (alist-ref 'fetch-prices options))
                     (granularity granularity-in-slots)
                     (out-path (alist-ref 'out-path options)))
        (main)))))

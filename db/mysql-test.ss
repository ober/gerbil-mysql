;;; -*- Gerbil -*-
;;; © vyzo
;;; :std/db/mysql unit-test
(import :std/test
        :std/db/dbi
        :std/iter
        (only-in :std/srfi/19
                 make-date date?
                 date-nanosecond date-second
                 date-minute date-hour
                 date-day date-month date-year)
        ./mysql)
(export mysql-test test-setup! test-cleanup!)

(def db #f)
(def (test-setup!)
  (set! db (sql-connect mysql-connect host: "localhost" user: "test" passwd: "test" db: "test"))
  (with-catch void (cut sql-eval db "DROP TABLE Users"))
  (with-catch void (cut sql-eval db "DROP TABLE HitCount"))
  (with-catch void (cut sql-eval db "DROP TABLE TypeTest")))
(def (test-cleanup!)
  (with-catch void (cut sql-eval db "DROP TABLE Users"))
  (with-catch void (cut sql-eval db "DROP TABLE HitCount"))
  (with-catch void (cut sql-eval db "DROP TABLE TypeTest"))
  (sql-close db))

(def mysql-test
  (test-suite "test :std/db/mysql"
    (test-case "prepare tables"
      (let (stmt (sql-prepare db "CREATE TABLE Users (FirstName VARCHAR(20), LastName VARCHAR(20), Secret VARCHAR(20))"))
        (check (sql-exec stmt) => #!void)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "INSERT INTO Users (FirstName, LastName, Secret) VALUES (?, ?, ?)"))
        (sql-bind stmt "John" "Smith" "very secret")
        (check (sql-exec stmt) => #!void)
        (sql-reset stmt)
        (sql-clear stmt)
        (sql-bind stmt "Marc" "Smith" "oh so secret")
        (check (sql-exec stmt) => #!void)
        (sql-bind stmt "Minnie" "Smith" #f)
        (check (sql-exec stmt) => #!void)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "CREATE TABLE HitCount (User VARCHAR(20), Hits INTEGER)"))
        (check (sql-exec stmt) => #!void)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "INSERT INTO HitCount (User,Hits) VALUES (?, ?)"))
        (sql-bind stmt "john" 20)
        (check (sql-exec stmt) => #!void)
        (sql-finalize stmt)))

    (test-case "read and modify table"
      (let (stmt (sql-prepare db "SELECT * FROM Users"))
        (check (sql-query stmt) => '(#("John" "Smith" "very secret")
                                     #("Marc" "Smith" "oh so secret")
                                     #("Minnie" "Smith" #f)))
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT * FROM Users WHERE FirstName = ?"))
        (sql-bind stmt "John")
        (check (sql-query stmt) => '(#("John" "Smith" "very secret")))
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "DELETE FROM Users WHERE FirstName = ?"))
        (sql-bind stmt "Marc")
        (check (sql-exec stmt) => #!void)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT * FROM Users"))
        (check (sql-query stmt) => '(#("John" "Smith" "very secret")
                                     #("Minnie" "Smith" #f)))
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT * FROM HitCount"))
        (check (sql-query stmt) => '(#("john" 20)))
        (sql-finalize stmt)))

    ;; Extended test coverage for all data types
    (test-case "setup type test table"
      (sql-eval db "CREATE TABLE TypeTest (id INTEGER PRIMARY KEY AUTO_INCREMENT, col_int INTEGER, col_bigint BIGINT, col_float FLOAT, col_double DOUBLE, col_text VARCHAR(255), col_blob BLOB, col_datetime DATETIME, col_date DATE)")
      )

    (test-case "integer types: positive, negative, zero"
      ;; BUG-4 regression: negative integers must sign-extend correctly
      (let (stmt (sql-prepare db "INSERT INTO TypeTest (col_int) VALUES (?)"))
        (sql-bind stmt 42)
        (sql-exec stmt)
        (sql-reset stmt)
        (sql-clear stmt)
        (sql-bind stmt -1)
        (sql-exec stmt)
        (sql-reset stmt)
        (sql-clear stmt)
        (sql-bind stmt 0)
        (sql-exec stmt)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT col_int FROM TypeTest WHERE col_int IS NOT NULL ORDER BY id"))
        (check (sql-query stmt) => '(42 -1 0))
        (sql-finalize stmt))

      (sql-eval db "DELETE FROM TypeTest"))

    (test-case "bigint types: large values, negative"
      ;; BUG-3 regression: values > 2^31 must not be truncated
      (let (stmt (sql-prepare db "INSERT INTO TypeTest (col_bigint) VALUES (?)"))
        (sql-bind stmt 5000000000)    ; > 2^32
        (sql-exec stmt)
        (sql-reset stmt)
        (sql-clear stmt)
        (sql-bind stmt -5000000000)
        (sql-exec stmt)
        (sql-reset stmt)
        (sql-clear stmt)
        (sql-bind stmt 0)
        (sql-exec stmt)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT col_bigint FROM TypeTest WHERE col_bigint IS NOT NULL ORDER BY id"))
        (check (sql-query stmt) => '(5000000000 -5000000000 0))
        (sql-finalize stmt))

      (sql-eval db "DELETE FROM TypeTest"))

    (test-case "float/double types"
      ;; BUG-1 regression: binding a real? value must not crash
      (let (stmt (sql-prepare db "INSERT INTO TypeTest (col_double) VALUES (?)"))
        (sql-bind stmt 3.14159)
        (sql-exec stmt)
        (sql-reset stmt)
        (sql-clear stmt)
        (sql-bind stmt -2.5)
        (sql-exec stmt)
        (sql-reset stmt)
        (sql-clear stmt)
        (sql-bind stmt 0.0)
        (sql-exec stmt)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT col_double FROM TypeTest WHERE col_double IS NOT NULL ORDER BY id"))
        (let (rows (sql-query stmt))
          ;; Use approximate comparison for floating point
          (check (length rows) => 3)
          (check (< (abs (- (car rows) 3.14159)) 0.001) => #t)
          (check (< (abs (- (cadr rows) -2.5)) 0.001) => #t)
          (check (< (abs (caddr rows)) 0.001) => #t))
        (sql-finalize stmt))

      (sql-eval db "DELETE FROM TypeTest"))

    (test-case "blob read/write"
      ;; BUG-2 regression: reading a BLOB column must not crash
      (let* ((data (u8vector 0 1 2 3 255 254 128))
             (stmt (sql-prepare db "INSERT INTO TypeTest (col_blob) VALUES (?)")))
        (sql-bind stmt data)
        (sql-exec stmt)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT col_blob FROM TypeTest WHERE col_blob IS NOT NULL"))
        (let (rows (sql-query stmt))
          (check (length rows) => 1)
          (check (u8vector? (car rows)) => #t)
          (check (u8vector->list (car rows)) => '(0 1 2 3 255 254 128)))
        (sql-finalize stmt))

      (sql-eval db "DELETE FROM TypeTest"))

    (test-case "datetime types"
      (let* ((dt (make-date 0 30 15 10 25 12 2024 0))
             (stmt (sql-prepare db "INSERT INTO TypeTest (col_datetime) VALUES (?)")))
        (sql-bind stmt dt)
        (sql-exec stmt)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT col_datetime FROM TypeTest WHERE col_datetime IS NOT NULL"))
        (let (rows (sql-query stmt))
          (check (length rows) => 1)
          (let (result (car rows))
            (check (date? result) => #t)
            (check (date-year result) => 2024)
            (check (date-month result) => 12)
            (check (date-day result) => 25)
            (check (date-hour result) => 10)
            (check (date-minute result) => 15)
            (check (date-second result) => 30)))
        (sql-finalize stmt))

      (sql-eval db "DELETE FROM TypeTest"))

    (test-case "date type"
      (let* ((dt (make-date 0 0 0 0 15 6 2024 0))
             (stmt (sql-prepare db "INSERT INTO TypeTest (col_date) VALUES (?)")))
        (sql-bind stmt dt)
        (sql-exec stmt)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT col_date FROM TypeTest WHERE col_date IS NOT NULL"))
        (let (rows (sql-query stmt))
          (check (length rows) => 1)
          (let (result (car rows))
            (check (date? result) => #t)
            (check (date-year result) => 2024)
            (check (date-month result) => 6)
            (check (date-day result) => 15)))
        (sql-finalize stmt))

      (sql-eval db "DELETE FROM TypeTest"))

    (test-case "null values per type"
      (let (stmt (sql-prepare db "INSERT INTO TypeTest (col_int, col_bigint, col_double, col_text, col_blob, col_datetime) VALUES (?, ?, ?, ?, ?, ?)"))
        (sql-bind stmt #f #f #f #f #f #f)
        (sql-exec stmt)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT col_int, col_bigint, col_double, col_text, col_blob, col_datetime FROM TypeTest ORDER BY id DESC LIMIT 1"))
        (let (rows (sql-query stmt))
          (check (length rows) => 1)
          (check (vector-ref (car rows) 0) => #f)   ; col_int
          (check (vector-ref (car rows) 1) => #f)   ; col_bigint
          (check (vector-ref (car rows) 2) => #f)   ; col_double
          (check (vector-ref (car rows) 3) => #f)   ; col_text
          (check (vector-ref (car rows) 4) => #f)   ; col_blob
          (check (vector-ref (car rows) 5) => #f))  ; col_datetime
        (sql-finalize stmt))

      (sql-eval db "DELETE FROM TypeTest"))

    (test-case "column names"
      (let (stmt (sql-prepare db "SELECT col_int, col_text FROM TypeTest"))
        (check (sql-columns stmt) => '("col_int" "col_text"))
        (sql-finalize stmt)))

    (test-case "reset and rebind cycle"
      (let (stmt (sql-prepare db "INSERT INTO TypeTest (col_int, col_text) VALUES (?, ?)"))
        (sql-bind stmt 1 "first")
        (sql-exec stmt)
        (sql-reset stmt)
        (sql-clear stmt)
        (sql-bind stmt 2 "second")
        (sql-exec stmt)
        (sql-reset stmt)
        (sql-clear stmt)
        (sql-bind stmt 3 "third")
        (sql-exec stmt)
        (sql-finalize stmt))

      (let (stmt (sql-prepare db "SELECT col_int, col_text FROM TypeTest WHERE col_int IS NOT NULL ORDER BY col_int"))
        (check (sql-query stmt) => '(#(1 "first") #(2 "second") #(3 "third")))
        (sql-finalize stmt))

      (sql-eval db "DELETE FROM TypeTest"))

    (test-case "single-column query returns scalar"
      (sql-eval db "INSERT INTO TypeTest (col_int) VALUES (99)")
      (let (stmt (sql-prepare db "SELECT col_int FROM TypeTest WHERE col_int = 99"))
        (let (rows (sql-query stmt))
          (check (length rows) => 1)
          ;; Single column result should be unwrapped to a scalar, not a vector
          (check (car rows) => 99))
        (sql-finalize stmt))

      (sql-eval db "DELETE FROM TypeTest"))

    (test-case "empty result set"
      (let (stmt (sql-prepare db "SELECT col_int FROM TypeTest WHERE col_int = -999"))
        (check (sql-query stmt) => '())
        (sql-finalize stmt)))
    ))

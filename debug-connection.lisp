
(in-package :database-core)

;; to illustrate how to create a subclass of the database-connection-wrapper
;; AND to provide debugging of stuff let's do this...

(defclass debug-connection (database-connection-wrapper)
  ())

(defmethod pg-exec :before ((c debug-connection) &rest args)
  (format *standard-output*
          "Called pg-exec with ~S~%" args))


;; DATABASE_USER=vip DATABASE_NAME=vip ccl64 -e "(asdf:load-system :database-core)" -e "(use-package :database-core)" -e "(setf (configuration::configuration-parameter :database/connection-class) (find-class 'database-core:debug-connection))" -e '(format t "~S" (dquery "select 1+2"))' -e "(ccl:quit)"

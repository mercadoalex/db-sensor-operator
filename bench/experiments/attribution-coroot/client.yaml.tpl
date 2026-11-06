# One client pod. It waits for a shared wall-clock boundary so that two clients commit almost
# together, then sends BEGIN, an INSERT with a comment tag, a short sleep, and COMMIT as separate
# statements (psql reading stdin sends each one as its own protocol message).
apiVersion: v1
kind: Pod
metadata: { name: @NAME@, namespace: bench, labels: { experiment: attribution-coroot } }
spec:
  restartPolicy: Never
  containers:
    - name: client
      image: postgres:16-alpine
      env:
        - { name: PGHOST, value: bench-postgres }
        - { name: PGUSER, value: bench }
        - { name: PGDATABASE, value: bench }
        - { name: PGAPPNAME, value: @NAME@ }
        - name: PGPASSWORD
          valueFrom: { secretKeyRef: { name: bench-pg-creds, key: password } }
      command: ["sh", "-c"]
      args:
        - |
          sleep $(( @BOUNDARY@ - $(date +%s) ))
          printf 'begin;\ninsert into public.orders (customer, amount) values (@CUSTOMER@, 1.00) /* workload=@NAME@ */;\nselect pg_sleep(0.3);\ncommit;\n' | psql -v ON_ERROR_STOP=1 -q
          sleep 40

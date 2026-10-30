# Kubernetes container logs

[`logit.yaml`](logit.yaml) reads every container's log on a node from `/var/log/pods`, takes each
container's id from the kubelet's links in `/var/log/containers`, and sends it to Loki over OTLP. It works with containerd and CRI-O. Run one `logit` per node as a DaemonSet:

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: logit
spec:
  selector:
    matchLabels: { app: logit }
  # One pod per node at a time: two pods on a node would both read every file and share one
  # checkpoint.
  updateStrategy:
    type: RollingUpdate
    rollingUpdate: { maxSurge: 0, maxUnavailable: 1 }
  template:
    metadata:
      labels: { app: logit }
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: logit
          image: ghcr.io/ross/logit:latest
          args: ["run", "/etc/logit/logit.yaml"]
          env:
            - name: NODE_NAME
              valueFrom: { fieldRef: { fieldPath: spec.nodeName } }
          # The runtimes write each log file 0640, owned by root.
          securityContext: { runAsUser: 0 }
          readinessProbe:
            httpGet: { path: /readyz, port: 9600 }
          volumeMounts:
            - { name: config, mountPath: /etc/logit, readOnly: true }
            - { name: pods, mountPath: /var/log/pods, readOnly: true }
            - { name: containers, mountPath: /var/log/containers, readOnly: true }
            - { name: state, mountPath: /var/lib/logit }
      volumes:
        - name: config
          configMap: { name: logit }
        - name: pods
          hostPath: { path: /var/log/pods, type: Directory }
        - name: containers
          hostPath: { path: /var/log/containers, type: Directory }
        - name: state
          hostPath: { path: /var/lib/logit, type: DirectoryOrCreate }
```

Store `logit.yaml` in the `logit` ConfigMap:

```sh
kubectl create configmap logit --from-file=logit.yaml
```

The `state` volume holds the checkpoint and the disk spool. It's a `hostPath` so that both
outlive the pod: a new pod resumes reading where the old one stopped and sends what the old one
hadn't delivered.

[`docs/deploying.md`](../../docs/deploying.md)'s "Kubernetes container logs: `cri_in`" covers the
rest: what each event carries, the metrics to watch, and the input's limits.

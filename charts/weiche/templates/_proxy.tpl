{{- define "weiche.proxyTemplate" -}}
{{- $p := .Values.proxy -}}
{{- $port := $p.http3.advertisedPort -}}
{{- if and (ne $port nil) (or (lt (int $port) 1) (gt (int $port) 65535)) -}}
{{- fail "proxy.http3.advertisedPort must be between 1 and 65535" -}}
{{- end -}}
{{- if not (has $p.log.format (list "text" "json")) -}}
{{- fail "proxy.log.format must be text or json" -}}
{{- end -}}
{{- $extra := dict -}}
{{- range $p.extraEnv -}}
{{- if has .name (list "POD_UID" "WEICHE_SNAPSHOT" "WEICHE_PROVISIONER_BINDING" "WEICHE_HTTP3" "WEICHE_HTTP3_PORT") -}}
{{- fail (printf "proxy.extraEnv cannot override %s" .name) -}}
{{- end -}}
{{- $_ := set $extra .name true -}}
{{- end -}}
metadata:
  labels: {{ $p.podLabels | toJson }}
  annotations: {{ $p.podAnnotations | toJson }}
spec:
  nodeSelector: {{ merge (dict "kubernetes.io/os" "linux") $p.nodeSelector | toJson }}
  terminationGracePeriodSeconds: {{ $p.terminationGracePeriodSeconds }}
  {{- with .Values.imagePullSecrets }}
  imagePullSecrets: {{ . | toJson }}
  {{- end }}
  {{- if $p.hostNetwork }}
  hostNetwork: true
  dnsPolicy: {{ default "ClusterFirstWithHostNet" $p.dnsPolicy | quote }}
  {{- else }}
  {{- with $p.dnsPolicy }}
  dnsPolicy: {{ . | quote }}
  {{- end }}
  securityContext:
    sysctls:
      - name: net.ipv4.ip_unprivileged_port_start
        value: "0"
  {{- end }}
  {{- with $p.tolerations }}
  tolerations: {{ . | toJson }}
  {{- end }}
  {{- with $p.affinity }}
  affinity: {{ . | toJson }}
  {{- end }}
  {{- with $p.topologySpreadConstraints }}
  topologySpreadConstraints: {{ . | toJson }}
  {{- end }}
  {{- with $p.priorityClassName }}
  priorityClassName: {{ . | quote }}
  {{- end }}
  volumes:
    - name: run
      emptyDir: {medium: Memory}
  containers:
    - name: proxy
      image: {{ include "weiche.image" . | quote }}
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      args: ["proxy", "$(WEICHE_SNAPSHOT)"]
      env:
        - name: WEICHE_SNAPSHOT
          valueFrom: {fieldRef: {fieldPath: spec.serviceAccountName}}
        - name: WEICHE_PROVISIONER_BINDING
          value: {{ include "weiche.provisionerRole" . | quote }}
        - name: POD_UID
          valueFrom: {fieldRef: {fieldPath: metadata.uid}}
        {{- if $p.http3.enabled }}
        - name: WEICHE_HTTP3
          value: "true"
        {{- with $port }}
        - name: WEICHE_HTTP3_PORT
          value: {{ . | quote }}
        {{- end }}
        {{- end }}
        {{- if and $p.log.level (not (hasKey $extra "RUST_LOG")) }}
        - name: RUST_LOG
          value: {{ $p.log.level | quote }}
        {{- end }}
        {{- if and $p.log.format (not (hasKey $extra "WEICHE_LOG_FORMAT")) }}
        - name: WEICHE_LOG_FORMAT
          value: {{ $p.log.format | quote }}
        {{- end }}
        {{- with $p.extraEnv }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      resources: {{ $p.resources | toJson }}
      volumeMounts: [{name: run, mountPath: /run/weiche}]
      readinessProbe:
        exec: {command: ["/usr/local/bin/weiche", "ready"]}
        periodSeconds: 1
        failureThreshold: 1
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
          {{- if $p.hostNetwork }}
          add: ["NET_BIND_SERVICE"]
          {{- end }}
        seccompProfile: {{ $p.seccompProfile | toJson }}
{{- end -}}

{{- define "weiche.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "weiche.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "weiche.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag | toString) -}}
{{- end -}}

{{- define "weiche.gatewayClassName" -}}
{{- default (include "weiche.fullname" .) .Values.gatewayClass.name -}}
{{- end -}}

{{- define "weiche.controllerRole" -}}
{{- printf "%s-gateway-controller" (include "weiche.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "weiche.provisionerRole" -}}
{{- printf "%s-gateway-provisioner" (include "weiche.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "weiche.leaseName" -}}
{{- printf "%s-controller" (include "weiche.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "weiche.electionRole" -}}
{{- printf "%s-election" (include "weiche.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "weiche.settingsName" -}}
{{- printf "%s-settings" (include "weiche.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

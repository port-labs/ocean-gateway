{{/*
Expand the name of the chart.
*/}}
{{- define "ocean-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this
(by the DNS naming spec).
*/}}
{{- define "ocean-gateway.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ocean-gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "ocean-gateway.labels" -}}
helm.sh/chart: {{ include "ocean-gateway.chart" . }}
{{ include "ocean-gateway.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- range $key, $value := .Values.extraLabels }}
{{ $key }}: {{ $value | quote }}
{{- end }}
{{- end }}

{{/*
Selector labels — used in matchLabels and service selectors.
*/}}
{{- define "ocean-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ocean-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Resolved container image (repository:tag, tag defaults to appVersion).
*/}}
{{- define "ocean-gateway.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
ConfigMap name.
*/}}
{{- define "ocean-gateway.configMapName" -}}
{{- printf "%s-config" (include "ocean-gateway.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Secret name (only created when redis.password is set).
*/}}
{{- define "ocean-gateway.secretName" -}}
{{- printf "%s-secret" (include "ocean-gateway.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

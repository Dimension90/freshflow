{{- define "freshflow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "freshflow.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "freshflow.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "freshflow.labels" -}}
app.kubernetes.io/name: {{ include "freshflow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "freshflow.componentName" -}}
{{- printf "%s-%s" (include "freshflow.fullname" .root) .component | trunc 63 | trimSuffix "-" }}
{{- end }}

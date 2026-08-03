{{- define "phif.dbUrl" -}}
{{- if .Values.database.internal -}}
postgresql+psycopg://phif:phif@{{ .Release.Name }}-postgres:5432/phif
{{- else -}}
{{ .Values.database.url }}
{{- end -}}
{{- end -}}

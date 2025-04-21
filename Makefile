kinko_host="beta.data.npolar.no/-/api"
komainu_host="beta.data.npolar.no/-/auth"
app_token="YWRtaW5AZXhhbXBsZS5vcmc6MTIzNDEyMzQxMjM0MTIzNA=="
dataset_id="f446a2ad-37a7-45e7-8728-2f13be3444bb"

measurements:
	./get_measurements.sh $(komainu_host) $(kinko_host) $(dataset_id) $(app_token)
	./post_attachments.sh $(komainu_host) $(kinko_host) $(dataset_id) $(app_token)

clean:
	rm -f measurements/*.ndjson


.PHONY: measurements

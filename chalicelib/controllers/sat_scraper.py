SCRAPER_PATH = "/home/scraping/scraping"
INTERNAL_RES_PATH = "/home/scraping"
RES_PATH = "/home/scraping/res"
INTERNAL_WD = "/usr/src/myapp/public"
DOCKER_IMAGE = "php:7.4-cli"
SSH_SCRAPER_UUID_COMMAND = (
    f"docker run --rm "
    f'-v "{SCRAPER_PATH}":/usr/src/myapp '
    f"-v {RES_PATH}:{INTERNAL_RES_PATH} "
    f"-w {INTERNAL_WD} "
    f"{DOCKER_IMAGE} "
    "php {request_type} "
    "{rfc} {uuids} "
    '{passphrase} "{cert_file}" "{private_key}"'
)

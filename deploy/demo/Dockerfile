# Public demo image: the Forge image plus the entrypoint that seeds the two
# demo accounts before Forge listens on a reachable address.
# Built by .github/workflows/demo-testsprite.yml with this directory as the
# build context. See docs/demo-instance.md.

ARG FORGE_BASE_IMAGE
FROM ${FORGE_BASE_IMAGE}

COPY --chmod=0755 forge-demo-entrypoint.sh /usr/local/bin/forge-demo-entrypoint
COPY --chmod=0755 forge-demo-healthcheck.sh /usr/local/bin/forge-demo-healthcheck

# Identifies this build to the outside: Forge serves the web directory as
# static files, so the value is readable at /demo-build.txt without signing
# in. The pull-request check waits for it before it runs the suite.
# This RUN, and the `mode=0700` of the data filesystem in docker-compose.yml,
# rely on the base image running as root. A base image with a non-root USER
# needs `USER root` here and an owner on the tmpfs mounts.
ARG FORGE_DEMO_BUILD_ID=unknown
RUN printf '%s\n' "${FORGE_DEMO_BUILD_ID}" > "${FORGE_WEB_DIST_DIR}/demo-build.txt"

HEALTHCHECK --interval=10s --timeout=5s --retries=6 --start-period=240s \
  CMD ["forge-demo-healthcheck"]

ENTRYPOINT ["forge-demo-entrypoint"]
CMD []

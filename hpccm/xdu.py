"""HPCCM recipe for xdu, a filesystem indexer.

    https://github.com/xdu-project/xdu

    hpccm --recipe xdu.py --format docker > Dockerfile
    hpccm --recipe xdu.py --format singularity \
          --singularity-version=3.2 > xdu.def

Installs the four xdu binaries, plus man pages and shell completions, from
the upstream release tarball. The download runs in a throwaway first stage,
so curl stays out of the finished image. Both stages use the same Debian or
Ubuntu base image, which must have apt.

Being two-stage, the Singularity output needs --singularity-version=3.2.
Without it hpccm drops the second stage and emits a definition file that
builds only the downloader, so the recipe refuses to generate rather than let
that pass quietly.

The release binaries are built on Ubuntu 24.04. Measured against v0.5.1, xdu
needs glibc 2.34, xdu-find and xdu-rm need 2.38, and xdu-view needs 2.39,
hence the Debian trixie runtime (2.41) for an image holding all four.
Debian bookworm ships 2.36, so it runs xdu from a release tarball but not
the other three.

Options, as --userarg key=value:

    version=0.5.1                    release tag; v0.5.1 works too.
    prefix=/opt/xdu                  install prefix.
    runtime_base=debian:trixie-slim  base image; needs glibc >= 2.39.

The architecture is resolved inside the container, so nothing needs passing
when building on aarch64.
"""

PROJECT = 'https://github.com/xdu-project/xdu'
DIST = '/xdu-dist.tar.gz'      # the one file stage 0 hands to stage 1

# Release tag to install; override with --userarg version=1.2.3
VERSION = '0.5.1'

# hpccm cannot infer a debian: image's distro and warns; ubuntu selects apt.
DISTRO = 'ubuntu'

# Handle both v1.2 and 1.2
version = 'v' + USERARG.get('version', VERSION).lstrip('v')
prefix = USERARG.get('prefix', '/opt/xdu').rstrip('/') or '/'
runtime_base = USERARG.get('runtime_base', 'debian:trixie-slim')

release = '{}/releases/download/{}'.format(PROJECT, version)
tarball = 'xdu-{}-$(uname -m)-unknown-linux-gnu.tar.gz'.format(version)

if (hpccm.config.g_ctype == container_type.SINGULARITY and
        hpccm.config.g_singularity_version < Version('3.2')):
    raise RuntimeError('two-stage recipe: re-run with '
                       '--singularity-version=3.2')


# Stage 0 never executes the binaries, so its glibc does not matter; sharing
# stage 1's image means the build pulls one image instead of two.
Stage0 += baseimage(image=runtime_base, _as='build', _distro=DISTRO)
Stage0 += packages(apt=['ca-certificates', 'curl'])
Stage0 += shell(commands=[
    'curl -fsSL -o {} "{}/{}"'.format(DIST, release, tarball),
])

# Stage 1: the image that ships.
Stage1 += baseimage(image=runtime_base, _distro=DISTRO)
Stage1 += label(metadata={
    'org.opencontainers.image.source': PROJECT,
    'org.opencontainers.image.version': version,
})
# xdu-find, xdu-view and xdu-rm link libstdc++.so.6, from bundled DuckDB.
Stage1 += packages(apt=['ca-certificates', 'libstdc++6'])
Stage1 += copy(_from='build', src=DIST, dest=DIST)

Stage1 += shell(commands=[
    'mkdir -p {}'.format(prefix),
    'tar -xzf {} -C {}'.format(DIST, prefix),
    'rm -f {}'.format(DIST),
])

# Running each binary fails the build on a base image whose glibc is too old,
# instead of shipping an image that dies at first exec.
Stage1 += shell(commands=['{}/bin/xdu --version'.format(prefix),
                          '{}/bin/xdu-find --version'.format(prefix),
                          '{}/bin/xdu-view --version'.format(prefix),
                          '{}/bin/xdu-rm --version'.format(prefix)])
Stage1 += environment(variables={
    'PATH': '{}/bin:$PATH'.format(prefix),
    'MANPATH': '{}/share/man:$MANPATH'.format(prefix),
})

# Docker only. Singularity has no USER and runs as the invoking user.
if hpccm.config.g_ctype == container_type.DOCKER:
    Stage1 += shell(commands=['groupadd --system xdu',
                              'useradd --system --gid xdu --create-home xdu'])
    Stage1 += user(user='xdu')

Stage1 += runscript(commands=['{}/bin/xdu'.format(prefix)])

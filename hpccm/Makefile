.POSIX:

# -------------------------------------------------------------
# Generate container specs from an HPCCM recipe.
#
# <name>.py is the source; <name>.def and <name>.docker are generated
# from it, and <name>.sif is built from <name>.def.
#
# 'make help' lists the targets and shows the current variables.
# -------------------------------------------------------------

TARGET = xdu

HPCCM ?= hpccm
SINGULARITY_VERSION ?= 3.8

# Extra hpccm options, passed through verbatim, e.g.
# make USERARGS='--userarg version=1.2.3 prefix=/usr/local'
# Not a prerequisite, so changing it does not make a spec stale; rm it
# first, or use -B with GNU make.
USERARGS ?=

# apptainer is the current name of the runtime; older sites still ship it as
# singularity. POSIX '!=' assigns from shell output, so help can report what
# was found. Empty if neither is installed; the .sif rule checks.
APPTAINER != command -v apptainer || command -v singularity || true

# Checked in the recipes rather than up front, so a checkout whose specs are
# already current needs no hpccm at all. The leading @ survives expansion, so
# the check does not echo itself.
CHECK_HPCCM = @command -v $(HPCCM) >/dev/null || \
    { echo "$(HPCCM) not found; set HPCCM=/path/to/hpccm" >&2; exit 1; }

# Declare our suffixes for POSIX inference rules. A bare .SUFFIXES clears the
# built-in list; the second line sets ours, so no built-in rule stays live.
.SUFFIXES:
.SUFFIXES: .py .def .docker .sif .image

.PRECIOUS: $(TARGET).def $(TARGET).docker

.PHONY: all help sif image clean

all: $(TARGET).def $(TARGET).docker

# Required. GNU make deletes a spec it built on the way to a .sif or an image
# unless the name is mentioned above, so 'make other.sif' would discard
# other.def. Makes that keep intermediates ignore this line. Keep it below
# 'all', or a make that allows a dot-target as the default goal will pick it.
.SECONDARY:

help:
	@echo 'Targets:'
	@echo '  all              generate $(TARGET).def and $(TARGET).docker  (default)'
	@echo '  sif              build $(TARGET).sif'
	@echo '  image            build the $(TARGET) docker image'
	@echo '  <name>           generate <name>.def and <name>.docker'
	@echo '  <name>.def       Apptainer definition file only'
	@echo '  <name>.docker    Dockerfile only'
	@echo '  <name>.sif       build <name>.sif'
	@echo '  <name>.image     build the <name> docker image'
	@echo '  clean            remove $(TARGET).sif'
	@echo
	@echo 'Variables and their current values:'
	@echo '  HPCCM=$(HPCCM)'
	@echo '  SINGULARITY_VERSION=$(SINGULARITY_VERSION)'
	@echo '  APPTAINER=$(APPTAINER)'
	@echo '  USERARGS=$(USERARGS)'
	@echo
	@echo 'Changing USERARGS does not make a spec stale; rm the spec first,'
	@echo 'or use -B with GNU make.'

# Build commands are not silenced, so what make echoes is what runs.

# POSIX single suffix rule: 'make <name>' -> <name>.def and <name>.docker.
# No file named <name> is created, so it always runs.
.py:
	$(MAKE) $@.def $@.docker

# POSIX double suffix rule: .py -> .def
.py.def:
	$(CHECK_HPCCM)
	$(HPCCM) --recipe $< --format singularity \
	    --singularity-version=$(SINGULARITY_VERSION) $(USERARGS) > $@

# POSIX double suffix rule: .py -> .docker
.py.docker:
	$(CHECK_HPCCM)
	$(HPCCM) --recipe $< --format docker $(USERARGS) > $@

# POSIX double suffix rule: .def -> .sif
.def.sif:
	@test -n "$(APPTAINER)" || \
	    { echo "no apptainer or singularity on PATH" >&2; exit 1; }
	$(APPTAINER) build --force $@ $<

# POSIX double suffix rule: .docker -> .image
.docker.image:
	docker build -t $* - < $<

sif: $(TARGET).sif

image: $(TARGET).image

clean:
	rm -f $(TARGET).sif

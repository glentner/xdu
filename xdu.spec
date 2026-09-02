Name:           xdu
Version:        0.4.1
Release:        1%{?dist}
Summary:        High-performance file system indexer for large-scale storage administration
License:        GPL
BuildRequires:       cargo
BuildRequires:       rust
BuildRequires:       gcc

%description
Extreme-scale parallel "du" command with search and TUI viewer.

%prep

%install
cargo install --root %{buildroot}/%{_prefix} --git https://github.com/glentner/xdu.git --tag v%{version}
# Clean up unwanted cargo artifacts
rm -f %{buildroot}/%{_prefix}/.crates.toml
rm -f %{buildroot}/%{_prefix}/.crates2.json
rm -f %{buildroot}/%{_bindir}/gen-completions

%clean
rm -rf $RPM_BUILD_ROOT

%files
%{_bindir}/%{name}
%{_bindir}/%{name}-find
%{_bindir}/%{name}-rm
%{_bindir}/%{name}-view

%changelog
* Wed Sep  02 2026 Geoffrey Lentner <glentner@purdue.edu> - 0.4.1
- First version being packaged


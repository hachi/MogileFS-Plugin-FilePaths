name:      perl-MogileFS-Plugin-FilePaths
summary:   perl-MogileFS-Plugin-FilePaths - MogileFS Tracker plugin to represent a traditional unix filesystem in mogilefs.
version:   0.02
release:   1
vendor:    Jonathan Steinert <hachi@cpan.org>
packager:  Jonathan Steinert <hachi@cpan.org>
license:   Artistic
group:     Applications/CPAN
buildroot: %{_tmppath}/%{name}-%{version}-%(id -u -n)
buildarch: noarch
source:    MogileFS-Plugin-FilePaths-%{version}.tar.gz
requires:  perl-MogileFS-Plugin-FilePaths

%description
MogileFS Tracker plugin to represent a traditional unix filesystem in mogilefs.

%prep
rm -rf "%{buildroot}"
%setup -n MogileFS-Plugin-FilePaths-%{version}

%build
%{__perl} Makefile.PL PREFIX=%{buildroot}%{_prefix}
make all
make test

%install
make pure_install

[ -x /usr/lib/rpm/brp-compress ] && /usr/lib/rpm/brp-compress


# remove special files
find %{buildroot} \(                    \
       -name "perllocal.pod"            \
    -o -name ".packlist"                \
    -o -name "*.bs"                     \
    \) -exec rm -f {} \;

# no empty directories
find %{buildroot}%{_prefix}             \
    -type d -depth -empty               \
    -exec rmdir {} \;

%clean
[ "%{buildroot}" != "/" ] && rm -rf %{buildroot}

%files
%defattr(-,root,root)
%{_prefix}/lib/*

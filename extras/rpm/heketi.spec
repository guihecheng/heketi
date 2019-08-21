%if 0%{?fedora}
%global with_devel 1
%global with_python 1
%global with_bundled 1
%global with_debug 0
%global with_check 1
%global with_unit_test 1
%else
%global with_devel 0
%global with_python 0
%global with_bundled 1
%global with_debug 0
# there is a race in the test-cases:
# https://github.com/heketi/heketi/issues/1468
%global with_check 0
%global with_unit_test 0
%endif

# Determine if systemd will be used
%if ( 0%{?fedora} && 0%{?fedora} > 16 ) || ( 0%{?rhel} && 0%{?rhel} > 6 )
%global with_systemd 1
%endif

%if 0%{?with_debug}
%global _dwz_low_mem_die_limit 0
%else
%global debug_package   %{nil}
%endif

%global provider        github
%global provider_tld    com
%global project         heketi
%global repo            heketi
# https://github.com/heketi/heketi
%global provider_prefix %{provider}.%{provider_tld}/%{project}/%{repo}
%global import_path     %{provider_prefix}

Name:           %{repo}
Version:        9.0.0
Release:        1%{?dist}
Summary:        RESTful based volume management framework for GlusterFS
License:        LGPLv3+ and GPLv2
URL:            https://%{provider_prefix}
Source0:        https://%{provider_prefix}/archive/v%{version}/%{name}-v%{version}.tar.gz
Source1:        https://%{provider_prefix}/releases/download/v%{version}/%{name}-deps-v%{version}.tar.gz
Source2:        %{name}.json
Source3:        %{name}.service
Source4:        %{name}.initd

# go-toolset SCLo is not available for non-x86_64 architectures
ExclusiveArch:  x86_64
BuildRequires:  go-toolset-7-golang-bin
BuildRequires:  go-toolset-7-runtime
BuildRequires:  openssl-devel
Requires(pre):  shadow-utils

%if 0%{?with_systemd}
BuildRequires:  systemd
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd
%else
Requires(post):   /sbin/chkconfig
Requires(preun):  /sbin/service
Requires(preun):  /sbin/chkconfig
Requires(postun): /sbin/service
%endif

%description
Heketi provides a RESTful management interface which can be used to manage
the life cycle of GlusterFS volumes.  With Heketi, cloud services like
OpenStack Manila, Kubernetes, and OpenShift can dynamically provision
GlusterFS volumes with any of the supported durability types.  Heketi
will automatically determine the location for bricks across the cluster,
making sure to place bricks and its replicas across different failure
domains.  Heketi also supports any number of GlusterFS clusters, allowing
cloud services to provide network file storage without being limited to a
single GlusterFS cluster.

%if 0%{?with_devel}
%package devel
Summary:       %{summary}
BuildArch:     noarch

%if 0%{?with_check} && ! 0%{?with_bundled}
BuildRequires: golang(github.com/auth0/go-jwt-middleware)
BuildRequires: golang(github.com/boltdb/bolt)
BuildRequires: golang(github.com/codegangsta/negroni)
BuildRequires: golang(github.com/dgrijalva/jwt-go)
BuildRequires: golang(github.com/gorilla/context)
BuildRequires: golang(github.com/gorilla/mux)
BuildRequires: golang(github.com/lpabon/godbc)
BuildRequires: golang(golang.org/x/crypto/ssh)
BuildRequires: golang(golang.org/x/crypto/ssh/agent)
%endif

%if 0%{?with_bundled} && 0%{?fedora}
Provides: bundled(golang(github.com/auth0/go-jwt-middleware)) = 8c897f7c3631a9e9405b9496fd8ce241acdef230
Provides: bundled(golang(github.com/boltdb/bolt)) = 980670afcebfd86727505b3061d8667195234816
Provides: bundled(golang(github.com/codegangsta/negroni)) = c7477ad8e330bef55bf1ebe300cf8aa67c492d1b
Provides: bundled(golang(github.com/dgrijalva/jwt-go)) = 5ca80149b9d3f8b863af0e2bb6742e608603bd99
Provides: bundled(golang(github.com/gorilla/context)) = 215affda49addc4c8ef7e2534915df2c8c35c6cd
Provides: bundled(golang(github.com/gorilla/mux)) = f15e0c49460fd49eebe2bcc8486b05d1bef68d3a
Provides: bundled(golang(github.com/lpabon/godbc)) = 9577782540c1398b710ddae1b86268ba03a19b0c
Provides: bundled(golang(golang.org/x/crypto/ssh)) = fcdb74e78f2621098ebc0376bbadffcf580ccfe4
Provides: bundled(golang(golang.org/x/crypto/ssh/agent)) = fcdb74e78f2621098ebc0376bbadffcf580ccfe4
%endif

Provides:      golang(%{import_path}/apps) = %{version}-%{release}
Provides:      golang(%{import_path}/apps/glusterfs) = %{version}-%{release}
Provides:      golang(%{import_path}/client/api/go-client) = %{version}-%{release}
Provides:      golang(%{import_path}/client/cli/go/commands) = %{version}-%{release}
Provides:      golang(%{import_path}/executors) = %{version}-%{release}
Provides:      golang(%{import_path}/executors/mockexec) = %{version}-%{release}
Provides:      golang(%{import_path}/executors/sshexec) = %{version}-%{release}
Provides:      golang(%{import_path}/middleware) = %{version}-%{release}
Provides:      golang(%{import_path}/rest) = %{version}-%{release}
Provides:      golang(%{import_path}/tests) = %{version}-%{release}
Provides:      golang(%{import_path}/utils) = %{version}-%{release}
Provides:      golang(%{import_path}/utils/ssh) = %{version}-%{release}

%description devel
%{summary}

This package contains library source intended for
building other packages which use import path with
%{import_path} prefix.
%endif

%if 0%{?with_unit_test} && 0%{?with_devel}
%package unit-test-devel
Summary:         Unit tests for %{name} package

%if 0%{?with_check}
#Here comes all BuildRequires: PACKAGE the unit tests
#in %%check section need for running
%endif

# test subpackage tests code from devel subpackage
Requires:        %{name}-devel = %{version}-%{release}

%description unit-test-devel
%{summary}

This package contains unit tests for project
providing packages with %{import_path} prefix.
%endif

%package client
Summary:        Command line client for Heketi
Group:          System Environment/Libraries
License:        LGPLv3+ and GPLv2

%description client
%{summary}

Command line program to interact with Heketi

%package templates
Summary:        Heketi and GlusterFS templates for Heketi
Group:          System Environment/Libraries
License:        ASL 2.0

%description templates
%{summary}

Heketi and GlusterFS templates for Heketi

%if 0%{with_python}
%package -n python-heketi
Summary:        Python libraries for Heketi
Group:          System Environment/Libraries
License:        ASL 2.0 and LGPLv3+
Requires:       python-jwt
Requires:       python-requests
BuildRequires:  python-setuptools
BuildRequires:  python2-devel

%description -n python-heketi
%{summary}

This package contains python libraries for interacting with Heketi
%endif

%enable_gotoolset7
%{?!gopath: %global gopath %{_datadir}/gocode}

%prep
%setup -q

%build
mkdir -p src/%{provider}.%{provider_tld}/%{project}
ln -s $(pwd) src/%{provider}.%{provider_tld}/%{project}/%{repo}

# ! Bundled
%if ! 0%{?with_bundled}
export GOPATH=$(pwd):%{gopath}
export LDFLAGS="-X main.HEKETI_VERSION=%{version}"
go build -o %{name}

export LDFLAGS="-X main.HEKETI_CLI_VERSION=%{version}"
cd client/cli/go
go build -o %{name}-cli
cd ../../..
%else

# Bundled

# workaround for vendor directory which doesn't have src
# which is needed by the GOPATH
mkdir -p ./src
tar -xvf %{SOURCE1} -C ./src

# Setup GOPATH
export GOPATH=$(pwd):%{gopath}

%define gohash %(head -c20 /dev/urandom | od -An -tx1 | tr -d '\ \\n')

# -s strips debug information
%if 0%{?rhel} && 0%{?rhel} > 6
go build -ldflags "-X main.HEKETI_VERSION=%{version} -B 0x%{gohash} -s -extldflags '-z relro -z now'" -o %{name}

cd client/cli/go
go build -ldflags "-X main.HEKETI_CLI_VERSION=%{version} -B 0x%{gohash} -s -extldflags '-z relro -z now'" -o %{name}-cli
cd ../../..
%else
go build -ldflags "-X main.HEKETI_VERSION=%{version} -B 0x%{gohash} -s -extldflags '-z relro -z now'" -o %{name}

cd client/cli/go
go build -ldflags "-X main.HEKETI_CLI_VERSION=%{version} -B 0x%{gohash} -s -extldflags '-z relro -z now'" -o %{name}-cli
cd ../../..
%endif

%endif

# Python
%if 0%{with_python}
cd client/api/python
%{__python2} setup.py build
%endif

%install
# Python
%if 0%{with_python}
cd client/api/python
%{__python2} setup.py install -O1 --skip-build --root %{buildroot}
cd ../../..
%endif

install -D -p -m 0755 client/cli/go/%{name}-cli.sh \
  %{buildroot}%{_datadir}/bash-completion/completions/%{name}-cli.sh
install -D -p -m 0755 %{name} %{buildroot}%{_bindir}/%{name}
install -D -p -m 0755 client/cli/go/%{name}-cli %{buildroot}%{_bindir}/%{name}-cli
install -d -m 0755 %{buildroot}%{_sysconfdir}/%{name}
install -m 644 -t %{buildroot}%{_sysconfdir}/%{name} %{SOURCE2}
install -D -p -m 0644 docs/man/heketi-cli.8 %{buildroot}%{_mandir}/man8/heketi-cli.8
install -D -p -m 0644 client/cli/go/topology-sample.json \
  %{buildroot}%{_datadir}/%{name}/topology-sample.json

install -D -p -m 0644 extras/openshift/templates/glusterfs-template.json \
  %{buildroot}%{_datadir}/%{name}/templates/glusterfs-template.json
install -D -p -m 0644 extras/openshift/templates/heketi-template.json \
  %{buildroot}%{_datadir}/%{name}/templates/heketi-template.json
install -D -p -m 0644 extras/openshift/templates/deploy-heketi-template.json \
  %{buildroot}%{_datadir}/%{name}/templates/deploy-heketi-template.json
install -D -p -m 0644 extras/openshift/endpoint/sample-gluster-endpoint.json \
  %{buildroot}%{_datadir}/%{name}/openshift/sample-gluster-endpoint.json
install -D -p -m 0644 extras/openshift/service/sample-gluster-service.json \
  %{buildroot}%{_datadir}/%{name}/openshift/sample-gluster-service.json

%if 0%{?with_systemd}
install -D -p -m 0644 %{SOURCE3} %{buildroot}%{_unitdir}/%{name}.service
%else
install -D -p -m 0755 %{SOURCE4} %{buildroot}%{_sysconfdir}/init.d/%{name}
%endif

# And create /var/lib/heketi
install -d -m 0755 %{buildroot}%{_sharedstatedir}/%{name}


# source codes for building projects
%if 0%{?with_devel}
install -d -p %{buildroot}/%{gopath}/src/%{import_path}/
echo "%%dir %%{gopath}/src/%%{import_path}/." >> devel.file-list
# find all *.go but no *_test.go files and generate devel.file-list
for file in $(find . -iname "*.go" \! -iname "*_test.go") ; do
    echo "%%dir %%{gopath}/src/%%{import_path}/$(dirname $file)" >> devel.file-list
    install -d -p %{buildroot}/%{gopath}/src/%{import_path}/$(dirname $file)
    cp -pav $file %{buildroot}/%{gopath}/src/%{import_path}/$file
    echo "%%{gopath}/src/%%{import_path}/$file" >> devel.file-list
done
%endif

# testing files for this project
%if 0%{?with_unit_test} && 0%{?with_devel}
install -d -p %{buildroot}/%{gopath}/src/%{import_path}/
# find all *_test.go files and generate unit-test-devel.file-list
for file in $(find . -iname "*_test.go"); do
    echo "%%dir %%{gopath}/src/%%{import_path}/$(dirname $file)" >> devel.file-list
    install -d -p %{buildroot}/%{gopath}/src/%{import_path}/$(dirname $file)
    cp -pav $file %{buildroot}/%{gopath}/src/%{import_path}/$file
    echo "%%{gopath}/src/%%{import_path}/$file" >> unit-test-devel.file-list
done
%endif

%if 0%{?with_devel}
sort -u -o devel.file-list devel.file-list
%endif

%check
%if 0%{?with_check} && 0%{?with_unit_test} && 0%{?with_devel}

%if ! 0%{?with_bundled}
export GOPATH=%{buildroot}/%{gopath}:%{gopath}
%gotest %{import_path}/apps/glusterfs
%gotest %{import_path}/client/api/go-client
%gotest %{import_path}/middleware
%else
export GOPATH=$(pwd):%{gopath}
go test -v %{import_path}/apps/glusterfs
go test -v %{import_path}/client/api/go-client
go test -v %{import_path}/middleware
go test -v %{import_path}/executors/kubeexec
go test -v %{import_path}/executors/sshexec
%endif
%endif

%pre
getent group %{name} >/dev/null || groupadd -r %{name}
getent passwd %{name} >/dev/null || useradd -r -g %{name} -d %{_sharedstatedir}/%{name} -s /sbin/nologin -c "heketi user" %{name}

%post
%if 0%{?with_systemd}
%systemd_post %{name}.service
%else
/sbin/chkconfig --add %{name}
%endif

%preun
%if 0%{?with_systemd}
%systemd_preun %{name}.service
%else
/sbin/service %{name} stop &> /dev/null
%endif

%postun
%if 0%{?with_systemd}
%systemd_postun %{name}.service
%else
/sbin/chkconfig --del %{name}
%endif


#define license tag if not already defined
%{!?_licensedir:%global license %doc}

%files
%license LICENSE
%doc README.md AUTHORS
%config(noreplace) %{_sysconfdir}/%{name}
%{_bindir}/%{name}
%dir %attr(-,%{name},%{name}) %{_sharedstatedir}/%{name}
%if 0%{?with_systemd}
%{_unitdir}/%{name}.service
%else
%{_sysconfdir}/init.d/%{name}
%endif

%if 0%{with_python}
%files -n python-heketi
%license LICENSE
%doc README.md AUTHORS
%{python_sitelib}/heketi
%{python_sitelib}/heketi-*.egg-info
%endif

%files client
%license LICENSE
%doc README.md AUTHORS
%{_bindir}/%{name}-cli
%{_mandir}/man8/heketi-cli.8*
%{_datadir}/%{name}/topology-sample.json
%{_datadir}/bash-completion/completions/%{name}-cli.sh

%files templates
%license LICENSE
%doc README.md AUTHORS
%{_datadir}/%{name}/templates/*
%{_datadir}/%{name}/openshift/*

%if 0%{?with_devel}
%files devel -f devel.file-list
%license LICENSE
%doc README.md AUTHORS
%dir %{gopath}/src/%{provider}.%{provider_tld}/%{project}
%dir %{gopath}/src/%{import_path}
%endif

%if 0%{?with_unit_test} && 0%{?with_devel}
%files unit-test-devel -f unit-test-devel.file-list
%license LICENSE
%doc README.md AUTHORS
%endif

%changelog
* Fri Apr 12 2019 Niels de Vos <ndevos@redhat.com> - 9.0.0-1
- Release 9.0.0
- Use SCL go-toolset for building
- Build for x86_64 only due to missing go-toolset on other arches

* Thu Sep 13 2018 Niels de Vos <ndevos@redhat.com> - 8.0.0-1
- Release 8.0.0

* Thu Jul 26 2018 Niels de Vos <ndevos@redhat.com> - 7.0.0-2
- Update heketi.service (Pete Travis PR#4)

* Tue Jun 5 2018 Niels de Vos <ndevos@redhat.com> - 7.0.0-1
- Release 7.0.0

* Fri Feb 23 2018 Niels de Vos <ndevos@redhat.com> - 6.0.0-1
- Release 6,0.0 final
- Do not build python-heketi

* Tue Dec 19 2017 Niels de Vos <ndevos@redhat.com> - 5.0.1-1
- Release 5.0.1 final

* Tue Sep 19 2017 Michael Adam <obnox@samba.org> - 5.0.0-1
- Release 5 final

* Thu May 25 2017 Jose A. Rivera <jarrpa@redhat.com> - 4.0.0-1
- Release 4 Final

* Fri Feb 10 2017 Fedora Release Engineering <releng@fedoraproject.org> - 3.0.0-3
- Rebuilt for https://fedoraproject.org/wiki/Fedora_26_Mass_Rebuild

* Fri Oct 28 2016 Jose A. Rivera <jarrpa@redhat.com> - 3.0.0-2
- Add full RELRO support

* Wed Oct 12 2016 Luis Pabón <lpabon@redhat.com> - 3.0.0-1
- Release 3 Final

* Sat Aug 06 2016 Luis Pabón <lpabon@redhat.com> - 2.0.6-1
- Release 2 Final

* Thu Jul 21 2016 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 2.0.3-3
- https://fedoraproject.org/wiki/Changes/golang1.7

* Tue Jul 19 2016 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 2.0.3-2
- https://fedoraproject.org/wiki/Changes/Automatic_Provides_for_Python_RPM_Packages

* Mon Jun 20 2016 Luis Pabón <lpabon@redhat.com> - 2.0.2-3
- Fixed glusterfs templates

* Mon Jun 13 2016 lpabon <lpabon@redhat.com> - 2.0.1-2
- Updated deploy-heketi template

* Mon Jun 13 2016 lpabon <lpabon@redhat.com> - 2.0.1-1
- Support for Heketi Storage in OpenShift

* Thu Jun 02 2016 lpabon <lpabon@redhat.com> - 2.0.0-2
- Do not create devel or unit_test packages in RHEL

* Thu Jun 02 2016 lpabon <lpabon@redhat.com> - 2.0.0-1
- Release 2.0.0

* Tue May 24 2016 lpabon <lpabon@redhat.com> - 1.4.2-1
- Update to the latest template and cli changes

* Tue May 24 2016 lpabon <lpabon@redhat.com> - 1.4.0-2
- Add patch to use downstream RHGS containers

* Tue May 24 2016 lpabon <lpabon@redhat.com> - 1.4.0-1
- Able to talk to /hello w/o authentication
- Heketi-cli can now create PV specs
- Templates

* Tue May 10 2016 lpabon <lpabon@redhat.com> - 1.3.0-1
- Kube exec support

* Tue May 03 2016 lpabon <lpabon@redhat.com> - 1.2.0-2
- Remove dependency on clients

* Sun May 01 2016 lpabon <lpabon@redhat.com> - 1.2.0-1
- Created client and python rpms

* Sun Apr 03 2016 lpabon <lpabon@redhat.com> - 1.0.2-4
- Update godeps and strip bundled build

* Mon Feb 22 2016 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 1.0.2-3
- https://fedoraproject.org/wiki/Changes/golang1.6

* Wed Feb 03 2016 Fedora Release Engineering <releng@fedoraproject.org> - 1.0.2-2
- Rebuilt for https://fedoraproject.org/wiki/Fedora_24_Mass_Rebuild

* Thu Dec 03 2015 lpabon <lpabon@redhat.com> - 1.0.2-1
- Heketi 1.0.2

* Tue Nov 03 2015 lpabon <lpabon@redhat.com> - 1.0.1-1
- Heketi 1.0.1

* Mon Oct 12 2015 lpabon <lpabon@redhat.com> - 1.0.0-1
- First package for Fedora



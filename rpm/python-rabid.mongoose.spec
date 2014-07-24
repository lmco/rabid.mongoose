%define name python-rabid.mongoose
%define version 0.2
%define unmangled_version 0.2
%define unmangled_version 0.2
%define release 4

Summary: A REST interface for searching pymongor 
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
Source1: rabid.mongoose.wsgi 
Source2: rabid.mongoose.conf
Source3: rabid-mongoose-broker.upstart
Source4: rmongoose.conf

License: MIT
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Lockheed Martin
Url: https://github.com/lmco/rabid.mongoose
Requires: mod_wsgi python-pymongor python-flask python-werkzeug, python-jinja2-26, upstart
%description
UNKNOWN

%prep
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}
cp -fp %{SOURCE1} ./
cp -fp %{SOURCE2} ./
cp -fp %{SOURCE3} ./
cp -fp %{SOURCE4} ./
%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
#%_libdir points to /usr/lib64, hardcoding because I cant figure out the right macro to use
ln -sf /usr/lib/python2.6/site-packages/rabidmongoose/tool.py $RPM_BUILD_ROOT%{_bindir}/rabidmongoose

mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/www/rmongoose/
install -m 644 -p $RPM_BUILD_DIR/%{name}-%{unmangled_version}/rabid.mongoose.wsgi \
        $RPM_BUILD_ROOT%{_localstatedir}/www/rmongoose/rabid.mongoose.wsgi

mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/httpd/conf.d/
install -m 644 -p $RPM_BUILD_DIR/%{name}-%{unmangled_version}/rabid.mongoose.conf \
        $RPM_BUILD_ROOT%{_sysconfdir}/httpd/conf.d/rabid.mongoose.conf

mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/security/limits.d/
install -m 644 -p $RPM_BUILD_DIR/%{name}-%{unmangled_version}/rmongoose.conf \
        $RPM_BUILD_ROOT%{_sysconfdir}/security/limits.d/rmongoose.conf

mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/init/
install -m 655 -p $RPM_BUILD_DIR/%{name}-%{unmangled_version}/rabid-mongoose-broker.upstart \
        $RPM_BUILD_ROOT%{_sysconfdir}/init/rabid-mongoose-broker.conf



%clean
rm -rf $RPM_BUILD_ROOT


%pre
# Add the rmongoose user
/usr/sbin/useradd -c "rmongoose user" \
-s /sbin/nologin -r -d /home/rmongoose rmongoose 2> /dev/null || : 

%post
initctl reload-configuration

%files -f INSTALLED_FILES
%defattr(0744, rmongoose, rmongoose)
%{_bindir}/rabidmongoose
%{_sysconfdir}/init/rabid-mongoose-broker.conf
%{_localstatedir}/www/rmongoose/rabid.mongoose.wsgi
%config(noreplace) %{_sysconfdir}/httpd/conf.d/rabid.mongoose.conf
%{_sysconfdir}/init/rabid-mongoose-broker.conf
%{_sysconfdir}/security/limits.d/rmongoose.conf

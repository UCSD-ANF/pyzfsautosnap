Package {
  allow_virtual => true
}
if $::osfamily == 'RedHat' {
  $epel_src = $::operatingsystemmajorrelease ? {
    6       => 'http://download.fedoraproject.org/pub/epel/6/i386/epel-release-6-8.noarch.rpm',
    default => undef,
  }
  package { 'epel-release' :
    source => $epel_src,
  } ->
  package { 'dkms' : } ->
  package {'zfs-release':
    source => 'http://archive.zfsonlinux.org/epel/zfs-release.el6.noarch.rpm',
  } ->
  package {'kernel-devel': } ->
  package {'zfs': }
}



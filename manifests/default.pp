Package {
  allow_virtual => true
}
if $::osfamily == 'RedHat' {
  class {'zfsonlinux': }
  file { '/etc/yum.repos.d/anf.repo':
    source => '/vagrant/files/anf.repo',
  }

}

file { '/export':
  ensure => 'directory',
} ->
file { '/export/home':
  ensure => 'directory',
}

node 'target.test.int' {
  exec { 'create poolfile':
    command => '/bin/dd if=/dev/zero of=/zfsbackups-pool-file bs=1024 count=1024k',
    creates => '/zfsbackups-pool-file',
  } ->
  zpool { 'zfsbackups':
    ensure => present,
    disk   => '/zfsbackups-pool-file',
  } ->
  class { 'zfsautosnap::server' :
    client_ssh_pubkey      => '/vagrant/files/client_id_dsa.pub',
    client_ssh_pubkey_type => 'ssh-dss',
  }
}

node 'client.test.int' {
  # type doesn't set ensure by default
  Zfs { ensure => 'present' }
  Exec { path  => '/sbin:/bin:/usr/sbin:/usr/bin' }

  exec { 'create poolfile':
    command => '/bin/dd if=/dev/zero of=/clienttest-pool-file bs=1024 count=1024k',
    creates => '/clienttest-pool-file',
  } ->
  zpool { 'clienttest':
    ensure => present,
    disk   => '/clienttest-pool-file',
  } ->
  zfs { 'clienttest': }

  class { 'zfsautosnap::client':
    target_hostname           => '192.168.34.3',
    client_ssh_privkey_source => '/vagrant/files/client_id_dsa',
  }

  # Puppet type doesn't handle spaces correctly
  #zfs { 'clienttest/crap with spaces':
  #  require => Zfs['clienttest'],
  #}

  zfs { 'clienttest/nodaily': }
  #exec { 'zfs set com.sun:auto-snapshot=true clienttest/nodaily':
  #  require      => Zfs['clienttest/nodaily'],
  #  unless       => 'test "`zfs list -Ho com.sun:auto-snapshot clienttest/nodaily`" == "true"',
  #}
  site::zfsuserprop{ 'clienttest/nodaily auto-snap':
    zfsdataset => 'clienttest/nodaily',
    property   => 'com.sun:auto-snapshot',
    value      => 'true',
  }
  site::zfsuserprop{ 'clienttest/nodaily auto-snap daily':
    zfsdataset => 'clienttest/nodaily',
    property   => 'com.sun:auto-snapshot:daily',
    value      => 'false',
  }
  #exec { 'zfs set com.sun:auto-snapshot:daily=false clienttest/nodaily':
  #  refreshonly => true,
  #  subscribe   => Zfs['clienttest/nodaily'],
  #}

  zfs { 'clienttest/snapnorecurse': } ~>
  exec { 'zfs set com.sun:auto-snapshot=true clienttest/snapnorecurse':
    refreshonly => true,
  }
  zfs { 'clienttest/snapnorecurse/child1': } ~>
  exec { 'zfs set com.sun:auto-snapshot=false clienttest/snapnorecurse/child1':
    refreshonly => true,
  }
  zfs { 'clienttest/snapnorecurse/child2': } ~>
  exec { 'zfs set com.sun:auto-snapshot=false clienttest/snapnorecurse/child2':
    refreshonly => true,
  }

  zfs { 'clienttest/snaprecurse': } ~>
  exec { 'zfs set com.sun:auto-snapshot=true clienttest/snaprecurse':
    refreshonly => true,
  }
  zfs { 'clienttest/snaprecurse/child1': } ~>
  exec { 'zfs inherit com.sun:auto-snapshot clienttest/snaprecurse/child1':
    refreshonly => true,
  }
  zfs { 'clienttest/snaprecurse/child2': }
  exec { 'zfs inherit com.sun:auto-snapshot clienttest/snaprecurse/child2':
    refreshonly => true,
  }
}

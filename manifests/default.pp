Package {
  allow_virtual => true
}
if $::osfamily == 'RedHat' {
  class {'zfsonlinux': }

}
node 'target.test.int' {
  exec { 'create poolfile':
    command => '/bin/dd if=/dev/zero of=/zfsbackups-pool-file bs=1024 count=1024k',
    creates => '/zfsbackups-pool-file',
  } ->
  zpool { 'zfsbackups':
    ensure => present,
    disk   => '/zfsbackups-pool-file',
  }
}

node 'client.test.int' {
  exec { 'create poolfile':
    command => '/bin/dd if=/dev/zero of=/clienttest-pool-file bs=1024 count=1024k',
    creates => '/clienttest-pool-file',
  } ->
  zpool { 'clienttest':
    ensure => present,
    disk   => '/clienttest-pool-file',
  }
}

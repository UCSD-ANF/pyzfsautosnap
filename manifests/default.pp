Package {
  allow_virtual => true
}
if $::osfamily == 'RedHat' {
  class {'zfsonlinux': }
}



define site::zfsuserprop (
  $ensure = 'present',
  $property,
  $zfsdataset,
  $value=undef,
) {

  validate_re($ensure,'(pre|ab)sent')
  if $ensure == 'present' and $value == undef {
    fail('A value must be provided when ensure==present')
  }

  validate_re($property,'[A-Za-z0-9.,]+:[A-Za-z0-9.,:]+')

  $value_real    = shellquote($value)
  if $ensure == 'absent' {
    exec { "zfs inherit \"${property}\" \"${zfsdataset}\"":
      refreshonly => true,
      subscribe   => Zfs[$zfsdataset],
    }
  } else {
    exec { "zfs set ${property}=\"${value_real}\" ${zfsdataset}":
      unless =>
      "test \"`zfs list -Ho ${property} ${zfsdataset}`\" == $value_real",
      require => Zfs[$zfsdataset],
    }
  }
}

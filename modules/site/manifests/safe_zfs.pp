# Per http://docs.oracle.com/cd/E19253-01/819-5461/gdrcw/index.html
# We need all ZFS properties to be less that 1024 characters.
# This defined type wraps calls to zfs {} so that we can use rspec
# to confirm/deny we're providing proper lengths of strings to share
# in zfs {}.  If we don't do this, the client will fail when it calls zfs
# rather than when we check it in rspec.  Ticket filed with Puppet at
# http://projects.puppetlabs.com/issues/21535 .  If/when its resolved,
# we can unwrap our zfs calls.
define site::safe_zfs(
  $autosnap       = undef,
  # per http://docs.puppetlabs.com/references/latest/type.html#zfs
  $aclinherit     = undef,
  $aclmode        = undef,
  $atime          = undef,
  $canmount       = undef,
  $checksum       = undef,
  $compression    = undef,
  $copies         = undef,
  $dedup          = undef,
  $devices        = undef,
  $ensure         = undef,
  $exec           = undef,
  $logbias        = undef,
  $mountpoint     = undef,
  $nbmand         = undef,
  $primarycache   = undef,
  $provider       = undef,
  $quota          = undef,
  $readonly       = undef,
  $recordsize     = undef,
  $refquota       = undef,
  $refreservation = undef,
  $reservation    = undef,
  $secondarycache = undef,
  $setuid         = undef,
  $shareiscsi     = undef,
  $sharenfs       = undef,
  $sharesmb       = undef,
  $snapdir        = undef,
  $version        = undef,
  $volsize        = undef,
  $vscan          = undef,
  $xattr          = undef,
  $zoned          = undef,
) {
  validate_re($::osfamily,'^(FreeBSD|Solaris)$')
  $zfsmaxlen = 1024

  # Ensure all property values are under our valid length.

  validate_slength($mountpoint,     $zfsmaxlen)
  validate_slength($name,           $zfsmaxlen)
  validate_slength($provider,       $zfsmaxlen)
  validate_slength($quota,          $zfsmaxlen)
  validate_slength($recordsize,     $zfsmaxlen)
  validate_slength($refquota,       $zfsmaxlen)
  validate_slength($refreservation, $zfsmaxlen)
  validate_slength($reservation,    $zfsmaxlen)
  validate_slength($secondarycache, $zfsmaxlen)
  validate_slength($setuid,         $zfsmaxlen)
  validate_slength($shareiscsi,     $zfsmaxlen)
  validate_slength($sharenfs,       $zfsmaxlen)
  validate_slength($sharesmb,       $zfsmaxlen)
  validate_slength($snapdir,        $zfsmaxlen)
  validate_slength($version,        $zfsmaxlen)
  validate_slength($volsize,        $zfsmaxlen)

  # Sometimes, this is done by matching a much shorter regex with ^$.
  # Note that undef evaluates to '', so our regexes reflect this.
  validate_re($aclinherit,
    '^(|discard|noallow|restricted|passthrough|passthrough-x)$')
  validate_re($aclmode,      '^(|discard|groupmask|passthrough)$')
  validate_re($atime,        '^(|on|off)$')
  validate_re($canmount,     '^(|on|off|noauto)$')
  validate_re($checksum,     '^(|on|off|fletcher2|fletcher4|sha256)$')
  validate_re($compression,  '^(|on|off|lzjb|gzip|gzip\-[1-9]|zle)$')
  validate_re($copies,       '^(|[1-3])$')
  validate_re($dedup,        '^(|on|off)$')
  validate_re($devices,      '^(|on|off)$')
  validate_re($ensure,       '^(|present|absent)$')
  validate_re($exec,         '^(|on|off)$')
  validate_re($logbias,      '^(|latency|throughput)$')
  validate_re($nbmand,       '^(|on|off)$')
  validate_re($primarycache, '^(|all|none|metadata)$')
  validate_re($readonly,     '^(|on|off)$')
  validate_re($vscan,        '^(|on|off)$')
  validate_re($xattr,        '^(|on|off)$')
  validate_re($zoned,        '^(|on|off)$')

  # Set custom property, com.sun:auto-snapshot.
  if 'bool' == type3x($autosnap) {
    $val = $autosnap ? {
      true  => inline_template('<%= true.to_s  %>'),
      false => inline_template('<%= false.to_s %>'),
    }
    exec { "zfs set com.sun:auto-snapshot=${val} ${name}":
      path    => ['/sbin','/bin'],
      unless  =>
      "test \"`zfs list -Ho com.sun:auto-snapshot ${name}`\" == '${val}'",
      require => Zfs[$name],
    }
  }

  # Proceed with nominal ZFS setup.
  zfs { $name :
    ensure         => $ensure,
    aclinherit     => $aclinherit,
    aclmode        => $aclmode,
    atime          => $atime,
    canmount       => $canmount,
    checksum       => $checksum,
    compression    => $compression,
    copies         => $copies,
    # Need Puppet3 for dedup
    #dedup          => $dedup,
    devices        => $devices,
    exec           => $exec,
    logbias        => $logbias,
    mountpoint     => $mountpoint,
    nbmand         => $nbmand,
    primarycache   => $primarycache,
    provider       => $provider,
    quota          => $quota,
    readonly       => $readonly,
    recordsize     => $recordsize,
    refquota       => $refquota,
    refreservation => $refreservation,
    reservation    => $reservation,
    secondarycache => $secondarycache,
    setuid         => $setuid,
    shareiscsi     => $shareiscsi,
    sharenfs       => $sharenfs,
    sharesmb       => $sharesmb,
    snapdir        => $snapdir,
    version        => $version,
    volsize        => $volsize,
    vscan          => $vscan,
    xattr          => $xattr,
    zoned          => $zoned,
  }
}

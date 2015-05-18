# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.box = 'puppetlabs/centos-6.6-64-puppet'

  config.vm.provision "puppet" do |puppet|
    puppet.module_path = "modules"
  end

  config.vm.define 'client' do |clienthost|
    clienthost.vm.hostname = 'client.test.int'
    clienthost.vm.network 'private_network', ip: '192.168.34.2'
  end

  config.vm.define "target" do |targethost|
    targethost.vm.hostname = 'target.test.int'
    targethost.vm.network 'private_network', ip: '192.168.34.3'
  end

end

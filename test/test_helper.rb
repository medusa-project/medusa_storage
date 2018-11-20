require_relative '../lib/medusa_storage.rb'
require 'minitest/autorun'
require 'minitest/spec'
require 'minitest/pride'

system('docker restart medusa-storage-s3-server')
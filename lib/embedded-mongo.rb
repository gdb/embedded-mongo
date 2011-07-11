require 'logger'
require 'rubygems'
require 'mongo'

$:.unshift(File.dirname(__FILE__))

module EmbeddedMongo
  def self.log
    unless @log
      @log = Logger.new(STDOUT)
      @log.level = Logger::WARN
    end
    @log
  end
end

require 'embedded-mongo/backend'
require 'embedded-mongo/backend/collection'
require 'embedded-mongo/backend/db'
require 'embedded-mongo/backend/manager'
require 'embedded-mongo/connection'
require 'embedded-mongo/collection'
require 'embedded-mongo/cursor'
require 'embedded-mongo/db'
require 'embedded-mongo/util'

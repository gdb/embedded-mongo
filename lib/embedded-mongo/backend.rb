module EmbeddedMongo
  module Backend
    @@backends = {}

    def self.connect_backend(spec)
      @@backends[spec] ||= Backend::Manager.new(spec)
    end
  end
end

module Myreplicator
  module ApplicationHelper

    def sortable(column, title = nil)
      title ||= column.titleize
      css_class = column == sort_column ? "current #{sort_direction}" : nil
      direction = column == sort_column && sort_direction == "asc" ? "desc" : "asc"
      link_to content_tag(:span, title), {:sort => column, :direction => direction}, {:class => css_class}
    end

    def chronos(secs)
      [[60, :seconds], [60, :minutes], [24, :hours], [1000, :days]].map{ |count, name|
        if secs > 0
          secs, n = secs.divmod(count)
          "#{n.to_i} #{name}"
        end
      }.compact.reverse.join(', ')
    end

    def export_err_count
      count = Log.where("state = 'error' AND job_type = 'export'").count
    end


    def run_count
      total = Log.where(:state => 'running').count
    end

    def loader_err_count
      count = Log.where("state = 'error' AND job_type = 'loader'").count
    end
    
    def transporter_err_count
      count = Log.where("state = 'error' AND job_type = 'transporter'").count
    end
    
    def under_maintenance
      #@redis = Redis.new(:host => 'localhost', :port => 6379)
      @redis = Redis.new(:host => Settings[:redis][:host], :port => Settings[:redis][:port])
      tmp = @redis.get "under_maintenance"
      puts tmp.class
      if tmp == "true"
        puts "===== ADASHKJDHJASKHDLASJLKDJALKS ====="
        puts tmp
        return true
      end
      return false
    end
    
  end
end

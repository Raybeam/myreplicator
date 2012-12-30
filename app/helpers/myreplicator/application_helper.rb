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

    def err_count
      total = Log.where(:state => 'error').count
      return total
    end


    def run_count
      total = Log.where(:state => 'running').count
      return total
    end

  end
end

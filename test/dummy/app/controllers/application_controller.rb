class ApplicationController < ActionController::Base
  protect_from_forgery
  
  puts request.url
  def print_a
    puts "Prints from dummy app"
  end

end

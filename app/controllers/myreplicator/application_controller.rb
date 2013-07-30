module Myreplicator
  class ApplicationController < ActionController::Base
    before_filter :authenticated?
    
    private 
    
    def authenticated?
      Kernel.p Myreplicator.auth_required
      if Myreplicator.auth_required
        puts request.fullpath
        url = "#{Myreplicator.login_redirect}?redirect_url=#{request.fullpath}"
        redirect_to url unless Myreplicator.authenticated
      end
    end
    
  end
end

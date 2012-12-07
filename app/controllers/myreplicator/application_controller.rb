module Myreplicator
  class ApplicationController < ActionController::Base
    before_filter :authenticated?
    
    private 

    def authenticated?
      if Myreplicator.auth_required
        redirect_to Myreplicator.login_redirect unless Myreplicator.authenticated
      end
    end
    
  end
end

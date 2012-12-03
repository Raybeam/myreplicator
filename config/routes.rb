Myreplicator::Engine.routes.draw do
  resources :exports
  root :to => "home#index"
  match '/errors', :to => "home#errors", :as => 'errors'
end

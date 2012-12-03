Myreplicator::Engine.routes.draw do
  # get "myreplicator" => "myreplicator/home#index" , :as => :myreplicator
  resources :exports
  root :to => "home#index"
  match '/errors', :to => "home#errors", :as => 'errors'
end

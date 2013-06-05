Myreplicator::Engine.routes.draw do
  resources :exports
  root :to => "home#index"
  match '/errors', :to => "home#errors", :as => 'errors'
  match '/kill/:id', :to => 'home#kill', :as => 'kill'
  
  resources :home do
    get :pause, :on => :collection
    get :resume, :on => :collection
  end
end

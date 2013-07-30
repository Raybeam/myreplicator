Myreplicator::Engine.routes.draw do
  resources :exports
  root :to => "home#index"
  match '/export_errors', :to => "home#export_errors", :as => 'export_errors'
  match '/transport_errors', :to => "home#transport_errors", :as => 'transport_errors'
  match '/load_errors', :to => "home#load_errors", :as => 'load_errors'
  match '/kill/:id', :to => 'home#kill', :as => 'kill'
  
  resources :home do
    get :pause, :on => :collection
    get :resume, :on => :collection
  end
  
  resources :exports do
    member do
      get 'reload'
    end
  end
end

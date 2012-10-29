Myreplicator::Engine.routes.draw do
  resources :exports

  root :to => "home#index"
end

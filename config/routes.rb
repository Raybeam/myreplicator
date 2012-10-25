Myreplicator::Engine.routes.draw do
  resources :exports

  root :to => "exports#index"
end

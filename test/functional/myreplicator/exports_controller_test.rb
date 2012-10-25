require 'test_helper'

module Myreplicator
  class ExportsControllerTest < ActionController::TestCase
    setup do
      @export = exports(:one)
    end
  
    test "should get index" do
      get :index
      assert_response :success
      assert_not_nil assigns(:exports)
    end
  
    test "should get new" do
      get :new
      assert_response :success
    end
  
    test "should create export" do
      assert_difference('Export.count') do
        post :create, export: {  }
      end
  
      assert_redirected_to export_path(assigns(:export))
    end
  
    test "should show export" do
      get :show, id: @export
      assert_response :success
    end
  
    test "should get edit" do
      get :edit, id: @export
      assert_response :success
    end
  
    test "should update export" do
      put :update, id: @export, export: {  }
      assert_redirected_to export_path(assigns(:export))
    end
  
    test "should destroy export" do
      assert_difference('Export.count', -1) do
        delete :destroy, id: @export
      end
  
      assert_redirected_to exports_path
    end
  end
end

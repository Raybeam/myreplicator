require_dependency "myreplicator/application_controller"

module Myreplicator
  class ExportsController < ApplicationController
    # GET /exports
    # GET /exports.json
    def index
      @exports = Export.all
  
      respond_to do |format|
        format.html # index.html.erb
        format.json { render json: @exports }
      end
    end
  
    # GET /exports/1
    # GET /exports/1.json
    def show
      @export = Export.find(params[:id])
  
      respond_to do |format|
        format.html # show.html.erb
        format.json { render json: @export }
      end
    end
  
    # GET /exports/new
    # GET /exports/new.json
    def new
      @export = Export.new
  
      respond_to do |format|
        format.html # new.html.erb
        format.json { render json: @export }
      end
    end
  
    # GET /exports/1/edit
    def edit
      @export = Export.find(params[:id])
    end
  
    # POST /exports
    # POST /exports.json
    def create
      @export = Export.new(params[:export])
  
      respond_to do |format|
        if @export.save
          format.html { redirect_to @export, notice: 'Export was successfully created.' }
          format.json { render json: @export, status: :created, location: @export }
        else
          format.html { render action: "new" }
          format.json { render json: @export.errors, status: :unprocessable_entity }
        end
      end
    end
  
    # PUT /exports/1
    # PUT /exports/1.json
    def update
      @export = Export.find(params[:id])
  
      respond_to do |format|
        if @export.update_attributes(params[:export])
          format.html { redirect_to @export, notice: 'Export was successfully updated.' }
          format.json { head :no_content }
        else
          format.html { render action: "edit" }
          format.json { render json: @export.errors, status: :unprocessable_entity }
        end
      end
    end
  
    # DELETE /exports/1
    # DELETE /exports/1.json
    def destroy
      @export = Export.find(params[:id])
      @export.destroy
  
      respond_to do |format|
        format.html { redirect_to exports_url }
        format.json { head :no_content }
      end
    end
  end
end

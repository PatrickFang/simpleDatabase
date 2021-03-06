require './command_parser'
require './simple_database'
require './command_executer'
require './command_buffer'
require 'fileutils'

temp_path          = './db/data.txt'
path_to_data       = './db/data/'
path_to_data_count = './db/count/'

#-------------comment out if need persistency
FileUtils.rm_rf(path_to_data) if File.directory?(path_to_data)
Dir.mkdir path_to_data 

FileUtils.rm_rf(path_to_data_count) if File.directory?(path_to_data_count)
Dir.mkdir path_to_data_count 
#-------------comment out if need persistency


$db = SimpleDatabase.new(temp_path, path_to_data, path_to_data_count)
$db.load_db

$nested_block_count = -1
operation = ""
command_buffer = CommandBuffer.new

while operation != 'END'
  command = gets

  if !command.nil?
    parsed_command = CommandParser.new(command)
    operation = parsed_command.operation
    key = parsed_command.key
    value = parsed_command.value

    command_buffer.process(operation, key, value)
  end
end

#-------------comment out if need persistency
FileUtils.rm_rf(path_to_data) if File.directory?(path_to_data)  #comment out if need persistency
FileUtils.rm_rf(path_to_data_count) if File.directory?(path_to_data_count) #comment out if need persistency
#-------------comment out if need persistency
require './command_parser'
require './simple_database'
require './command_executer'

temp_path          = './db/data.txt'
path_to_data       = './db/data/'
path_to_data_count = './db/count/'

$db = SimpleDatabase.new(temp_path, path_to_data, path_to_data_count)

$db.load_db
$db.print_data

operation = ""

while operation != 'END'
  command = gets

  parsed_command = CommandParser.new(command)
  operation = parsed_command.operation
  key = parsed_command.key
  value = parsed_command.value
  puts "commands: #{operation} #{key} #{value}"

  CommandExecuter.new(operation, key, value).excute
end
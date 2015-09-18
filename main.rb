require './command_parser'
require './simple_database'
require './command_executer'

$db = SimpleDatabase.new('./data.txt')
$db.load_data
$db.print_data

operation = ""
while operation != 'END'
  command = gets
  puts "Your entered: " + command

  parsed_command = CommandParser.new(command)
  operation = parsed_command.operation
  key = parsed_command.key
  value = parsed_command.value

  CommandExecuter.new(operation, key, value).excute



  puts "parsed commands: #{operation} #{key} #{value}"
end
require './command_parser'
require './simple_database'
require './command_executer'

temp_path          = './db/data.txt'
path_to_data       = './db/data/'
path_to_data_count = './db/count/'

$db = SimpleDatabase.new(temp_path, path_to_data, path_to_data_count)

$db.load_db
$db.print_data
$nested_block_count = -1

operation = ""
command_buffer = CommandBuffer.new
while operation != 'END'
  command = gets
  if command == "PEND"
    puts "waiting for new command"
    command = gets
  end

  in_transaction = false
  if !command.nil?
    parsed_command = CommandParser.new(command)
    operation = parsed_command.operation
    key = parsed_command.key
    value = parsed_command.value
    puts "commands: #{operation} #{key} #{value}"

    if operation == "BEGIN"
      in_transaction = true
      nested_block_count += 1
    elsif operation == "COMMIT"
      in_transaction = false
      nested_block_count = 0
    elsif operation == "ROLLBACK"
      if !in_transaction
        puts "NO TRANSACTION"
      else
        command_buffer.undo_last_block(nested_block_count)
        nested_block_count -= 1
      end
    else
      in_transaction = in_transaction
    end

    if in_transaction
      command_buffer.add(operation, key, value, nested_block_count)
    else
      command_buffer.run
    end

    #CommandExecuter.new(operation, key, value).excute
  end
end

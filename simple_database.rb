class CommandParser
  attr_reader :input

  def initialize(input)
    @input ||= input
  end

  def operation
    parsed_input[0] if parsed_input.length >= 1
  end

  def key
    parsed_input[1] if parsed_input.length >= 2
  end

  def value
    parsed_input[2] if parsed_input.length >= 3
  end

  private

  def parsed_input
    input.gsub!(/\n/, "")
    input.gsub(/\s+/m, ' ').strip.split(" ")
  end
end

class SimpleDatabase
  def initialize(on_disk_path = "data.txt")
  end


end




operation = ""
while operation != 'END'
  command = gets
  puts "Your entered: " + command

  parsed_command = CommandParser.new(command)
  operation = parsed_command.operation
  key = parsed_command.key
  value = parsed_command.value

  puts "parsed commands: #{operation} #{key} #{value}"

  case operation
  when "SET"
   puts 'im setting'
  when "GET"

  when "UNSET"

  when "NUMEQUALTO"

  else 
    'unknwo command'
  end

end

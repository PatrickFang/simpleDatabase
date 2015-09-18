class CommandExecuter
  attr_reader :operation, :key, :value

  def initialize(operation, key, value)
    @operation ||= operation
    @key       ||= key
    @value     ||= value
  end

  def excute
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
end

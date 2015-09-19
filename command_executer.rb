class CommandExecuter
  attr_reader :operation, :key, :value

  def initialize(operation, key, value)
    @operation ||= operation
    @key       ||= key
    @value     ||= value
  end

  def excute(in_transaction=false)
    case operation
    when "SET"
      $db.set(key, value)
      $db.create_or_update_file(key, value, true) unless in_transaction

      old_count = $db.get(value, false)
      #puts "old count: #{old_count}"
      new_count = old_count.to_i+1
      #puts "new count: #{new_count}"

      $db.set(value, new_count, false)
      $db.create_or_update_file(value, new_count, false) unless in_transaction
    when "GET"
      puts $db.get(key)
    when "UNSET"

    when "NUMEQUALTO"
      puts $db.get(key, false)
    when "BEGIN"

    when "ROLLBACK"

    when "COMMIT"

    else 
      'unknown command'
    end
  end
end

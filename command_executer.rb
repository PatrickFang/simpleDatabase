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
      new_count = old_count + 1
      $db.set(value, new_count, false)
      $db.create_or_update_file(value, new_count, false) unless in_transaction
    when "GET"
      puts $db.get(key)
    when "UNSET"
      $db.unset(key)
      value = $db.get(key, false)
      $db.unset(value, false)

      old_count = $db.get(value, false)
      new_count = old_count + 1
      $db.set(value, new_count, false)
      $db.create_or_update_file(value, new_count, false) unless in_transaction

      $db.delete_file(key) unless in_transaction
    when "NUMEQUALTO"
      puts $db.get(key, false)
    when "BEGIN"

    when "ROLLBACK"
      #revert the last transaction block
    when "COMMIT"
      #write to disk and close all transactions
    else 
      'unknown command'
    end
  end
end

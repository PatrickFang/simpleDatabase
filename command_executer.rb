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
      $db.create_or_update_file(key, value) unless in_transaction

      $db.set(value, 0, false)
      new_count = $db.get(value, false)
      $db.create_or_update_file(value, new_count, false) unless in_transaction
    when "GET"
      puts $db.get(key)
    when "UNSET"
      count_key = $db.get(key)
      $db.unset(count_key.to_s, false)
      $db.unset(key)

      $db.delete_data_file(key) unless in_transaction

      current_count = $db.get(count_key, false)
      $db.create_or_update_file(count_key, current_count, false) unless in_transaction
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

class CommandExecuter
  attr_reader :operation, :key, :value

  def initialize(operation, key, value=0)
    @operation ||= operation
    @key       ||= key
    @value     ||= value
  end

  def excute
    case operation
    when "COMMIT"
      $db.set(key, value)
      $db.create_or_update_file(key, value)
    when "SET"
      #decrement the count of the old value
      old_value = $db.get(key)
      $db.set(old_value, -1, false)
      new_count_for_old_value = $db.get(old_value, false)
      $db.create_or_update_file(old_value, new_count_for_old_value, false)

      #update value for the key
      $db.set(key, value)
      $db.create_or_update_file(key, value)

      #increment the count of the new value
      $db.set(value, 1, false)
      new_count = $db.get(value, false)
      $db.create_or_update_file(value, new_count, false)
    when "GET"
      return $db.get(key)
    when "UNSET_IN_TRANSACTION"
      if $db.get(key) != nil
        $db.unset(key)
        $db.delete_data_file(key)
      else
        "DOES NOT EXIST"
      end
    when "UNSET"
      if $db.get(key) != nil
        count_key = $db.get(key)
        $db.unset(count_key.to_s, false)
        $db.unset(key)

        $db.delete_data_file(key)

        current_count = $db.get(count_key, false)
        $db.create_or_update_file(count_key, current_count, false)
      else
        "DOES NOT EXIST"
      end
    when "NUMEQUALTO"
      return $db.get(key, false)
    else 
      'UNKNOWN COMMAND'
    end
  end
end

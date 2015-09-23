class CommandBuffer
  attr_accessor :data_change_history, :count_change_history, :blocks, :affected_data, :block_counter, :affected_count

  def initialize
    @data_change_history  ||= {}
    @count_change_history ||= {}
    @affected_data        ||= []
    @affected_count       ||= []
    @blocks               ||= []
    @block_counter          = -1
  end

  def process(operation, key, value)
    #non transaction
    if @block_counter == -1 && operation != "BEGIN"
      if operation == "GET" || operation == "NUMEQUALTO"
        value = CommandExecuter.new(operation, key, value).excute
        if value.nil?
          puts "NULL"
        else
          puts value
        end
      elsif operation == "ROLLBACK"
        puts "NO TRANSACTION"
      elsif operation == "COMMIT"
        #no-op
      else
        CommandExecuter.new(operation, key, value).excute
      end
    #transactions
    else
      if operation != "BEGIN"
        affected_data[@block_counter] = [] if affected_data[@block_counter].nil?
        affected_count[@block_counter] = [] if affected_count[@block_counter].nil?
      end

      if operation == "SET" || operation == "UNSET"
        in_transaction_process(operation, key, value, @block_counter)
      elsif operation == "GET"
        if get_from_buffer(key).nil?
          puts "NULL"
        else
          puts get_from_buffer(key)
        end
      elsif operation == "NUMEQUALTO"
        puts numequalto_from_buffer(key).to_s
      elsif operation == "COMMIT"
        commit
        @block_counter = -1
      elsif operation == "ROLLBACK"
        if @block_counter == -1
          "NO TRANSACTION"
        else
          rollback
          @block_counter -= 1
        end
      elsif operation == "BEGIN"
        @block_counter += 1
      end
    end
  end

  def in_transaction_process(operation, key, value, block_index)
    return if key.nil?

    #initialize
    data_change_history[key.to_sym] = [] if data_change_history[key.to_sym].nil?

    #start processing current transaction block
    #grab original value for current key
    original_value = get_from_buffer(key)
    decrement_count_for_value(value, original_value, block_index)
    affected_count[block_index] |= [original_value] unless original_value.nil?
    affected_count.compact!

    #update data history for current key
    update_data_history_for(key, value, block_index)

    #grab new value for current key
    increment_count_for_value(value, original_value, block_index)

    affected_data[block_index] |= [key]
    affected_data.compact!
    affected_count[block_index] |= [value] unless value.nil?
    affected_count.compact!
  end

  def commit
    #commit data change and commit part of the count change
    data_change_history.each do |key, change|
      next if change.nil? || change.empty?

      if change.last[:value].nil?
        original_value = CommandExecuter.new("UNSET_IN_TRANSACTION", key).excute        
      else
        CommandExecuter.new("COMMIT", key, change.last[:value]).excute
      end
    end
    #clear the entire data history because they are committed
    @data_change_history = {}
    @affected_data = []

    #commit the count changes
    count_change_history.each do |key, change|
      old_count = CommandExecuter.new("NUMEQUALTO", key).excute.to_i

      diff = change.nil? || change.empty? ? 0: change.last[:diff].to_i
      new_count = diff + old_count

      $db.set(key, diff, false)

      #write to file the new change
      $db.create_or_update_file(key, new_count, false)
    end
    #clear the entire count history because they are committed
    @count_change_history = {}
    @affected_count = []
  end

  def rollback
    #pop all changed values in affected keys
    affected_data.last.each do |key|
      data_change_history[key.to_sym].pop
    end
    affected_data.pop

    #undo all changes in affected_count
    affected_count.last.each do |key|
      count_change_history[key.to_sym].pop
    end
    affected_count.pop
  end

  def decrement_count_for_value(new_value, original_value, block_index)
    #decrement the count for current key in buffer, if it is not nil
    if original_value.nil?
      #do nothing because the original didn't exist
    else
      if new_value != original_value
        count_change_history[original_value.to_sym] = [] if count_change_history[original_value.to_sym].nil?
        history_for_original_value = count_change_history[original_value.to_sym]
        count_previous_index = history_for_original_value.empty? ? -1 : history_for_original_value.last[:block_index]
        last_change = history_for_original_value.empty? ? 0 : history_for_original_value.last[:diff]
        if block_index != count_previous_index
          #creates a new entry in history
          count_change_history[original_value.to_sym] << { diff: last_change - 1, block_index: block_index } 
        else
          #if the same block, the new values overwrites the last history
          count_change_history[original_value.to_sym].last[:diff] = last_change - 1
        end
      end
    end
  end

  def update_data_history_for(key, value, block_index)
    history_for_current_key = data_change_history[key.to_sym]
    data_previous_index = history_for_current_key.empty? ? -1 : history_for_current_key.last[:block_index]

    if block_index != data_previous_index
      data_change_history[key.to_sym] << { value: value, block_index: block_index }  
    else
      data_change_history[key.to_sym].last[:value] = value
    end
  end

  def increment_count_for_value(new_value, original_value, block_index)
    #increment the count for the new key in buffer, if it is not nil
    if new_value.nil?
      #no-op: don't need to track count for nil
    else
      if new_value != original_value
        count_change_history[new_value.to_sym] = [] if count_change_history[new_value.to_sym].nil?
        history_for_new_value = count_change_history[new_value.to_sym]
        count_previous_index = history_for_new_value.empty? ? -1 : history_for_new_value.last[:block_index]
        last_change = history_for_new_value.empty? ? 0 : history_for_new_value.last[:diff]
        if block_index != count_previous_index
          #creates a new entry in history
          count_change_history[new_value.to_sym] << { diff: last_change + 1, block_index: block_index }
        else
          #if the same block, the update overwrites the last history
          count_change_history[new_value.to_sym].last[:diff] = last_change + 1
        end
      end
    end
  end

  def get_from_buffer(key)
    if data_change_history[key.to_sym].nil? || data_change_history[key.to_sym].empty?
      return CommandExecuter.new("GET", key).excute
    else
      return data_change_history[key.to_sym].last[:value]
    end
  end

  def numequalto_from_buffer(key)
    initial_count = CommandExecuter.new("NUMEQUALTO", key).excute.to_i
    if count_change_history[key.to_sym].nil?
      initial_count
    else
      if count_change_history[key.to_sym].nil? || count_change_history[key.to_sym].empty?
        initial_count
      else
        initial_count + count_change_history[key.to_sym].last[:diff].to_i
      end
    end
  end
end

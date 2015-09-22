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
    if @block_counter == -1 && operation != "BEGIN"
      if operation == "GET" || operation == "NUMEQUALTO"
        value = CommandExecuter.new(operation, key, value).excute
        if value.nil?
          puts "NULL"
        else
          puts value
        end
      elsif operation == "ROLLBACK"
        "NO TRANSACTION"
      else
        CommandExecuter.new(operation, key, value).excute
      end
    else
      if operation == "SET" || operation == "UNSET"
        in_transaction_process(operation, key, value, @block_counter)
      elsif operation == "GET"
        if get_from_buffer(key).nil?
          puts "NULL"
        else
          puts get_from_buffer(key)
        end
      elsif operation == "NUMEQUALTO"
        puts "the key for numequalto_from_buffer is #{key}"
        puts numequalto_from_buffer(key)
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
        puts "current count: #{@block_counter}"
      end
    end
  end

  def in_transaction_process(operation, key, value, block_index)
    puts "data_change_history: #{data_change_history}"
    puts "count_change_history: #{count_change_history}"
    puts "affected_data: for block_index: #{block_index} key: #{key} #{affected_data[block_index]}"
    puts "affected_count: for block_index: #{block_index} key: #{key} #{affected_count[block_index]}"    

    affected_data[block_index] = [] if affected_data[block_index].nil?
    affected_data[block_index] |= [key]

    data_change_history[key.to_sym] = [] if data_change_history[key.to_sym].nil?
    history_for_current_key = data_change_history[key.to_sym]
    data_previous_index = history_for_current_key.empty? ? -1 : history_for_current_key.last[:block_index]

    affected_count[block_index] = [] if affected_count[block_index].nil?

    #grab original value for current key
    original_value = get_from_buffer(key)

    #decrement the count for current key in buffer, if it is not nil
    if original_value.nil?
      #do nothing because the original didn't exist
    else
      if value != original_value
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
        affected_count[block_index] |= [original_value]
      end
    end

    #update data history for current key
    if block_index != data_previous_index
      data_change_history[key.to_sym] << { value: value, block_index: block_index }  
    else
      data_change_history[key.to_sym].last[:value] = value
    end

    #grab new value for current key
    new_value = value

    #increment the count for the new key in buffer, if it is not nil
    if new_value.nil?
      #unsetting
      #do nothing because the new value is nil, don't need to track
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
        affected_count[block_index] |= [new_value]
      end
    end

    puts "data_change_history: #{data_change_history}"
    puts "count_change_history: #{count_change_history}"
    puts "affected_data: #{affected_data}"    
    puts "affected_count: #{affected_count}"    
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

    #commit the rest of count changes
    count_change_history.each do |key, change|
      #write to file the new change
      old_count = CommandExecuter.new("NUMEQUALTO", key).excute

      diff = change.last[:diff]
      new_count = diff + old_count
      $db.set(key, diff, false)

      puts "update #{key}'s count to #{old_count}+#{diff} = #{new_count} "
      $db.create_or_update_file(key, new_count, false)
      puts "validation in master #{$db.get(key, false)}"
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

  def get_from_buffer(key)
    if data_change_history[key.to_sym].nil? || data_change_history[key.to_sym].empty?
      return CommandExecuter.new("GET", key).excute
    else
      return data_change_history[key.to_sym].last[:value]
    end
  end

  def numequalto_from_buffer(key)
    initial_count = CommandExecuter.new("NUMEQUALTO", key).excute
    if count_change_history[key.to_sym].nil?
      initial_count
    else
      initial_count + count_change_history[key.to_sym].last[:diff]
    end
  end
end
